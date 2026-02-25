"""rename HSI backfill job and add daily 21:00 schedule

Revision ID: 0013_rename_hsi_turnover_job
Revises: 0012_job_definition
Create Date: 2026-02-25

"""

from __future__ import annotations

from alembic import op


revision = "0013_rename_hsi_turnover_job"
down_revision = "0012_job_definition"
branch_labels = None
depends_on = None


OLD_JOB = "backfill_hsi_am_from_kline"
NEW_JOB = "backfill_hsi_turnover_from_kline"


def upgrade() -> None:
    # Ensure new job definition exists with the target metadata.
    op.execute(
        f"""
        INSERT INTO job_definition (
            job_name,
            handler_name,
            label_zh,
            description_zh,
            targets,
            params_schema,
            default_params,
            is_active,
            manual_enabled,
            schedule_enabled,
            ui_order
        )
        SELECT
            '{NEW_JOB}',
            '{NEW_JOB}',
            '回填HSI成交(AM+FULL)',
            '回填 HSI 历史 AM turnover（12:00-12:15窗口）并同步更新当日 FULL turnover 到 index_quote_history。',
            '["index_realtime_snapshot", "index_realtime_api_snapshot", "index_quote_source_record", "index_quote_history"]'::jsonb,
            COALESCE(params_schema, '[]'::jsonb),
            COALESCE(default_params, '{{}}'::jsonb),
            is_active,
            manual_enabled,
            TRUE,
            ui_order
        FROM job_definition
        WHERE job_name = '{OLD_JOB}'
        ON CONFLICT (job_name) DO UPDATE SET
            handler_name = EXCLUDED.handler_name,
            label_zh = EXCLUDED.label_zh,
            description_zh = EXCLUDED.description_zh,
            targets = EXCLUDED.targets,
            params_schema = EXCLUDED.params_schema,
            default_params = EXCLUDED.default_params,
            is_active = EXCLUDED.is_active,
            manual_enabled = EXCLUDED.manual_enabled,
            schedule_enabled = EXCLUDED.schedule_enabled,
            ui_order = EXCLUDED.ui_order,
            updated_at = NOW()
        """
    )

    # Move schedules from old job name to new job name.
    op.execute(
        f"""
        UPDATE job_schedule
        SET job_name = '{NEW_JOB}', updated_at = NOW()
        WHERE job_name = '{OLD_JOB}'
        """
    )

    # Remove old definition once schedules are moved.
    op.execute(f"DELETE FROM job_definition WHERE job_name = '{OLD_JOB}'")

    # Keep only the 21:00 daily schedule for this job, then upsert it.
    op.execute(f"DELETE FROM job_schedule WHERE job_name = '{NEW_JOB}' AND schedule_code <> '2100'")
    op.execute(
        f"""
        INSERT INTO job_schedule (
            job_name,
            schedule_code,
            trigger_type,
            timezone,
            second,
            minute,
            hour,
            day,
            month,
            day_of_week,
            jitter_seconds,
            misfire_grace_time,
            coalesce,
            max_instances,
            is_active,
            description
        ) VALUES (
            '{NEW_JOB}',
            '2100',
            'cron',
            'Asia/Shanghai',
            '0',
            '0',
            '21',
            '*',
            '*',
            '*',
            NULL,
            600,
            TRUE,
            1,
            TRUE,
            '每日 21:00'
        )
        ON CONFLICT (job_name, schedule_code) DO UPDATE SET
            trigger_type = EXCLUDED.trigger_type,
            timezone = EXCLUDED.timezone,
            second = EXCLUDED.second,
            minute = EXCLUDED.minute,
            hour = EXCLUDED.hour,
            day = EXCLUDED.day,
            month = EXCLUDED.month,
            day_of_week = EXCLUDED.day_of_week,
            jitter_seconds = EXCLUDED.jitter_seconds,
            misfire_grace_time = EXCLUDED.misfire_grace_time,
            coalesce = EXCLUDED.coalesce,
            max_instances = EXCLUDED.max_instances,
            is_active = EXCLUDED.is_active,
            description = EXCLUDED.description,
            updated_at = NOW()
        """
    )

    op.execute(
        f"""
        UPDATE job_definition
        SET
            handler_name = '{NEW_JOB}',
            schedule_enabled = TRUE,
            updated_at = NOW()
        WHERE job_name = '{NEW_JOB}'
        """
    )


def downgrade() -> None:
    # Restore old job definition and map schedule back.
    op.execute(
        f"""
        INSERT INTO job_definition (
            job_name,
            handler_name,
            label_zh,
            description_zh,
            targets,
            params_schema,
            default_params,
            is_active,
            manual_enabled,
            schedule_enabled,
            ui_order
        )
        SELECT
            '{OLD_JOB}',
            '{OLD_JOB}',
            '回填HSI半日成交(由K线聚合)',
            '基于 index_kline_source_record(EASTMONEY,5m) 聚合回填 HSI 的历史 AM turnover 到 index_quote_history。',
            '["index_kline_source_record", "index_quote_source_record", "index_quote_history"]'::jsonb,
            COALESCE(params_schema, '[]'::jsonb),
            COALESCE(default_params, '{{}}'::jsonb),
            is_active,
            manual_enabled,
            FALSE,
            ui_order
        FROM job_definition
        WHERE job_name = '{NEW_JOB}'
        ON CONFLICT (job_name) DO UPDATE SET
            handler_name = EXCLUDED.handler_name,
            label_zh = EXCLUDED.label_zh,
            description_zh = EXCLUDED.description_zh,
            targets = EXCLUDED.targets,
            params_schema = EXCLUDED.params_schema,
            default_params = EXCLUDED.default_params,
            is_active = EXCLUDED.is_active,
            manual_enabled = EXCLUDED.manual_enabled,
            schedule_enabled = EXCLUDED.schedule_enabled,
            ui_order = EXCLUDED.ui_order,
            updated_at = NOW()
        """
    )
    op.execute(
        f"""
        UPDATE job_schedule
        SET job_name = '{OLD_JOB}', updated_at = NOW()
        WHERE job_name = '{NEW_JOB}'
        """
    )
    op.execute(f"DELETE FROM job_definition WHERE job_name = '{NEW_JOB}'")
    op.execute(
        f"""
        UPDATE job_definition
        SET handler_name = '{OLD_JOB}', schedule_enabled = FALSE, updated_at = NOW()
        WHERE job_name = '{OLD_JOB}'
        """
    )
