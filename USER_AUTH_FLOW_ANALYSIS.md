# User Authentication Flow Analysis - Market Turnover App

## Executive Summary

This document maps all user registration, login, and authentication code in the FastAPI application. The goal is to implement admin-controlled user activation: **new users register with `is_active=False` by default, and can only login when manually activated by admins**.

---

## 1. DATA MODEL: AppUser

**Location:** `/app/db/models.py` (lines 351-379)

```python
class AppUser(Base):
    __tablename__ = "app_user"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    username = Column(String(320), nullable=False)  # Must equal email (constraint)
    email = Column(String(320), nullable=False)     # Normalized to lowercase
    password_hash = Column(String(255), nullable=False)
    display_name = Column(String(64), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)  # ← KEY FIELD (currently True)
    is_superuser = Column(Boolean, nullable=False, default=False)
    last_login_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    
    # Indexes
    Index("ix_app_user_active", AppUser.is_active)
    Index("ix_app_user_created_at", AppUser.created_at.desc())
```

**Key Observations:**
- `is_active` currently defaults to `True` in the ORM model
- Database schema also has `DEFAULT TRUE` (set in migration 0007)
- There's an index on `is_active` for querying inactive users
- Email & username must match and be lowercase (constraints)

---

## 2. DATABASE MIGRATION: AppUser Table Creation

**Location:** `/migrations/versions/0007_user_visit_logs.py` (lines 20-46)

```python
def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS app_user (
          id BIGSERIAL PRIMARY KEY,
          username VARCHAR(320) NOT NULL,
          email VARCHAR(320) NOT NULL,
          password_hash VARCHAR(255) NOT NULL,
          display_name VARCHAR(64),
          is_active BOOLEAN NOT NULL DEFAULT TRUE,          # ← Currently defaults to TRUE
          is_superuser BOOLEAN NOT NULL DEFAULT FALSE,
          last_login_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          ...
        );
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_app_user_active ON app_user (is_active);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_app_user_created_at ON app_user (created_at DESC);")
    
    # Also creates user_visit_logs table for audit logging
```

**Modifications Needed:**
1. Change database default: `DEFAULT TRUE` → `DEFAULT FALSE`
2. Change ORM default: `default=True` → `default=False`

---

## 3. USER REGISTRATION FLOW

**Location:** `/app/web/routes.py` (lines 831-923)

### 3a. Registration Page (GET /register)
```python
@router.get("/register", response_class=HTMLResponse)
def register_page(request: Request, current_user: AppUser | None = Depends(get_current_user)):
    # Redirects to /jobs if already logged in
    if current_user is not None:
        return RedirectResponse(url=next_path, status_code=303)
    # Returns register.html form
```

### 3b. Registration Submit (POST /register)
**Lines 849-922** - **KEY FILE FOR MODIFICATION #1**

```python
@router.post("/register", response_class=HTMLResponse)
def register_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
    display_name: str = Form(""),
    next_path: str = Form("/jobs"),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    # Validation logic...
    
    user = AppUser(
        username=email_n,
        email=email_n,
        password_hash=hash_password(password),
        display_name=display_name or None,
        is_active=True,              # ← CHANGE THIS TO: is_active=False
        is_superuser=False,
        last_login_at=datetime.now(timezone.utc),  # ← Should NOT set on registration
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # Currently logs in the user immediately:
    set_login_cookie(response, int(user.id))  # ← This will fail after is_active=False
    return response
```

**Current Behavior:**
- Creates user with `is_active=True`
- Immediately sets login cookie (auto-login)
- Redirects to safe_next (default `/jobs`)

**Required Changes:**
1. Set `is_active=False` on registration
2. Do NOT set login cookie after registration (user cannot login until activated)
3. Show success message: "Registration successful. Admin approval pending."

---

## 4. LOGIN VALIDATION

**Location:** `/app/web/auth.py` (lines 112-119) & `/app/web/routes.py` (lines 936-1000)

### 4a. Authentication Check During Request
**File:** `/app/web/auth.py` lines 112-119

```python
def get_current_user(request: Request, db: Session = Depends(get_db)) -> AppUser | None:
    user_id = parse_session_user_id(request.cookies.get(AUTH_COOKIE_NAME))
    if user_id is None:
        return None
    user = db.query(AppUser).filter(AppUser.id == user_id).first()
    if user is None or not user.is_active:  # ← ALREADY CHECKS is_active!
        return None
    return user
```

**Good news:** This already validates `is_active` on every request! ✓

### 4b. Login Submit
**File:** `/app/web/routes.py` lines 936-1000 - **KEY FILE FOR MODIFICATION #2**

```python
@router.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next_path: str = Form("/jobs"),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    # Find user
    user = db.query(AppUser).filter(AppUser.email == email_n).first()
    
    # Check credentials
    if user is None or not verify_password(password, user.password_hash):
        return error_response("邮箱或密码错误。")  # Email or password incorrect
    
    # Check is_active ✓ ALREADY IMPLEMENTED
    if not user.is_active:
        return templates.TemplateResponse(
            "login.html",
            _template_context(
                request,
                current_user=None,
                next_path=safe_next,
                email=email_n,
                error="用户已被禁用。",  # User has been disabled
            ),
            status_code=403,
        )
    
    # If we reach here: credentials valid AND is_active=True
    user.last_login_at = datetime.now(timezone.utc)
    db.add(user)
    db.commit()
    
    # Set login cookie
    response = RedirectResponse(url=safe_next, status_code=303)
    set_login_cookie(response, int(user.id))
    return response
```

**Good news:** Login validation is already complete! ✓
- Checks credentials
- Checks `is_active`
- Returns appropriate error message if inactive

---

## 5. SESSION COOKIE & TOKEN MANAGEMENT

**Location:** `/app/web/auth.py` (lines 68-110)

### 5a. Create Session Token
```python
def create_session_token(user_id: int) -> str:
    exp_ts = int(time.time()) + int(settings.AUTH_SESSION_MAX_AGE_SECONDS)
    payload = f"{int(user_id)}:{exp_ts}".encode("utf-8")
    payload_b64 = _b64url_encode(payload)
    sig = hmac.new(_secret_bytes(), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{sig}"
```

### 5b. Parse Session Token
```python
def parse_session_user_id(token: str | None) -> int | None:
    # Validates HMAC signature
    # Checks expiration (exp_ts)
    # Returns user_id or None
```

### 5c. Set Login Cookie
```python
def set_login_cookie(response: Response, user_id: int) -> None:
    token = create_session_token(user_id)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,  # "mt_session"
        value=token,
        max_age=int(settings.AUTH_SESSION_MAX_AGE_SECONDS),
        httponly=True,
        samesite="lax",
        secure=False,  # Note: not HTTPS in dev
        path="/",
    )
```

---

## 6. PROTECTED ROUTES THAT REQUIRE LOGIN

Routes that check `get_current_user` and redirect to login if absent:

| Route | Location | Login Required |
|-------|----------|---|
| `GET /` (dashboard) | routes.py:715-730 | ✗ No (public) |
| `GET /recent` | routes.py:733-762 | ✓ Yes (line 739-740) |
| `GET /jobs` | routes.py:765-788 | ✓ Yes (line 771-772) |
| `POST /api/jobs/run` | routes.py:791-828 | ✓ Yes (line 798-800) |
| `GET /register` | routes.py:831-846 | ✗ No (but redirects if already logged in) |
| `POST /register` | routes.py:849-922 | ✗ No |
| `GET /login` | routes.py:925-933 | ✗ No |
| `POST /login` | routes.py:936-1000 | ✗ No |
| `POST /logout` | routes.py:1003-1014 | ✓ (logs event if user exists) |

---

## 7. UTILITY FUNCTIONS - PASSWORD & EMAIL

**Location:** `/app/web/auth.py`

### Password Hashing
```python
def hash_password(password: str) -> str:
    # PBKDF2-SHA256 with 260,000 iterations
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 260_000)
    return f"pbkdf2_sha256${260_000}${salt.hex()}${digest.hex()}"

def verify_password(password: str, password_hash: str) -> bool:
    # Parse hash, recompute digest, constant-time compare
```

### Email Normalization
```python
def normalize_email(email: str) -> str:
    return email.strip().lower()

def is_valid_email(email: str) -> bool:
    # Regex validation
```

---

## 8. AUDIT LOGGING

**Location:** `/app/web/routes.py` (lines 347-366)

```python
def _append_auth_visit_log(db: Session, request: Request, *, user_id: int, action_type: str) -> None:
    row = UserVisitLog(
        user_id=user_id,
        ip_address=_extract_client_ip_for_log(request),
        action_type=action_type,  # "register", "login", "logout"
        ...
    )
    db.add(row)
    db.commit()
```

**Current Logging:**
- Line 919: `_append_auth_visit_log(db, request, user_id=int(user.id), action_type="register")`
- Line 980: `_append_auth_visit_log(db, request, user_id=int(user.id), action_type="login")`
- Line 1010: `_append_auth_visit_log(db, request, user_id=int(current_user.id), action_type="logout")`

---

## 9. USER VISIT LOGS TABLE

**Location:** `/migrations/versions/0007_user_visit_logs.py` (lines 49-89)

```sql
CREATE TABLE user_visit_logs (
  id BIGSERIAL PRIMARY KEY,
  user_id INT,
  action_type VARCHAR(20),  -- 'login', 'logout', 'register'
  ip_address INET NOT NULL,
  session_id VARCHAR(100),
  user_agent TEXT,
  request_url TEXT NOT NULL,
  ...
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
```

---

## 10. REQUIRED ADMIN ENDPOINTS (NOT YET IMPLEMENTED)

**❌ Currently Missing:**
- No admin panel to view users
- No admin panel to activate/deactivate users
- No admin panel to view registration requests
- No user management endpoints

**Needed Endpoints:**
```
GET  /admin/users               -- List users with status (active/inactive)
POST /admin/users/{id}/activate -- Activate a user
POST /admin/users/{id}/deactivate -- Deactivate a user (revoke access)
POST /admin/users/{id}/delete   -- Delete a user
```

---

## MODIFICATION PLAN SUMMARY

### Phase 1: Change Defaults
1. **File:** `/app/db/models.py` line 370
   - Change: `is_active = Column(Boolean, nullable=False, default=True)`
   - To: `is_active = Column(Boolean, nullable=False, default=False)`

2. **File:** `/migrations/versions/0007_user_visit_logs.py` line 29
   - Change: `is_active BOOLEAN NOT NULL DEFAULT TRUE,`
   - To: `is_active BOOLEAN NOT NULL DEFAULT FALSE,`
   - OR create new migration file for this change

### Phase 2: Registration Flow
1. **File:** `/app/web/routes.py` lines 891-898
   - Remove automatic login: delete `set_login_cookie(response, int(user.id))`
   - Change `is_active=True` to `is_active=False`
   - Show success page with message: "Registration pending admin approval"
   - Don't redirect to `/jobs` (user cannot access it)

### Phase 3: Login Validation
- ✓ Already works! No changes needed
- `get_current_user()` in auth.py already checks `is_active`
- Login form already shows "用户已被禁用。" error

### Phase 4: Admin User Management (Future)
- Create admin panel routes
- Create activate/deactivate endpoints
- Add admin UI template

---

## FILE CHECKLIST

| File | Purpose | Modification Required |
|------|---------|---|
| `/app/db/models.py` | AppUser ORM model | Change `default=True` to `default=False` |
| `/migrations/versions/0007_user_visit_logs.py` | Schema migration | Change `DEFAULT TRUE` to `DEFAULT FALSE` |
| `/app/web/auth.py` | Session/token management | ✓ No changes needed |
| `/app/web/routes.py` | Registration & login handlers | Update registration flow |
| `/app/config.py` | Environment variables | May need new settings for admin |
| `/app/web/templates/register.html` | Registration form | Show pending approval message |
| `/app/main.py` | App initialization | ✓ No changes needed |

---

## TEST CASES

After modification:

1. **Register new user**
   - User created with `is_active=False` ✓
   - User cannot login (error: "User disabled") ✓
   - Entry in `user_visit_logs` with `action_type='register'` ✓

2. **Admin activates user**
   - Update: `UPDATE app_user SET is_active=TRUE WHERE id=X`
   - User can now login ✓
   - Entry in `user_visit_logs` with `action_type='login'` ✓

3. **Admin deactivates user**
   - Update: `UPDATE app_user SET is_active=FALSE WHERE id=X`
   - User's session token still valid but blocked by `get_current_user()` ✓
   - User cannot access protected routes ✓

---

## SQL QUERIES FOR ADMIN USE

```sql
-- View all pending users (inactive)
SELECT id, email, display_name, created_at 
FROM app_user 
WHERE is_active = FALSE 
ORDER BY created_at DESC;

-- View active users
SELECT id, email, display_name, last_login_at, created_at 
FROM app_user 
WHERE is_active = TRUE 
ORDER BY last_login_at DESC;

-- Activate a user
UPDATE app_user SET is_active = TRUE WHERE id = <user_id>;

-- Deactivate a user
UPDATE app_user SET is_active = FALSE WHERE id = <user_id>;

-- Delete a user and their logs
DELETE FROM user_visit_logs WHERE user_id = <user_id>;
DELETE FROM app_user WHERE id = <user_id>;

-- View login history
SELECT user_id, email, action_type, ip_address, request_url, created_at
FROM user_visit_logs
LEFT JOIN app_user ON user_visit_logs.user_id = app_user.id
WHERE action_type IN ('login', 'logout', 'register')
ORDER BY created_at DESC
LIMIT 100;
```

---

## ENVIRONMENT VARIABLES (Existing)

From `/app/config.py`:

- `AUTH_SECRET_KEY` - HMAC secret for session tokens
- `AUTH_SESSION_MAX_AGE_SECONDS` - Session duration (default: probably 7 days)
- `BASE_PATH` - URL prefix (e.g., `/market-turnover`)

---

## NOTES

- **Email constraint:** `username = email` (always equal, both lowercase)
- **Audit:** All login/register/logout events logged to `user_visit_logs`
- **Session format:** HMAC-signed base64 with expiration timestamp
- **No email verification:** Currently no email verification step (future enhancement)
- **No password reset:** Currently no password reset flow (future enhancement)
- **Superuser flag:** `is_superuser` field exists but not yet used (future for admin roles)

