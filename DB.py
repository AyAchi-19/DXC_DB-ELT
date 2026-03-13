import mysql.connector

DB_CONFIG = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
}
DB_NAME = 'dashboard_dxx'

TABLES = {}

# companies 
TABLES['companies'] = """
CREATE TABLE `companies` (
  `id`         INT AUTO_INCREMENT PRIMARY KEY,
  `name`       VARCHAR(255) NOT NULL,
  `slug`       VARCHAR(100) NOT NULL,
  UNIQUE KEY `uq_name` (`name`),
  UNIQUE KEY `uq_slug` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

TABLES['queue_config'] = """
CREATE TABLE `queue_config` (
  `id`             INT AUTO_INCREMENT PRIMARY KEY,
  `company_id`     INT          NOT NULL,
  `queue_name`     VARCHAR(255) NOT NULL,
  `desk`           VARCHAR(100) DEFAULT NULL,
  `language`       VARCHAR(30)  DEFAULT NULL,
  `sla_formula`    ENUM('SLA1','SLA2','SLA3'),
  `abd_formula`    ENUM('Abd1','Abd2','Abd3','Abd4','Abd5','NONE'),
  `tf_bh_sec`      SMALLINT     NOT NULL DEFAULT 30,
  `tf_ooh_sec`     SMALLINT     NOT NULL DEFAULT 30,
  `target_ans_pct` DECIMAL(5,2) DEFAULT NULL,
  `target_abd_pct` DECIMAL(5,2) DEFAULT NULL,
  `is_exempt`      TINYINT(1)   NOT NULL DEFAULT 0,
  UNIQUE KEY `uq_company_queue` (`company_id`, `queue_name`),
  INDEX `idx_qc_company` (`company_id`),
  FOREIGN KEY (`company_id`) REFERENCES `companies` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

_METRICS_COLS = """
  `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
  `queue_id`        INT      NOT NULL,

  `interval_start`  DATETIME NOT NULL,
  `interval_end`    DATETIME NOT NULL,
  `is_bh`           TINYINT(1) NOT NULL DEFAULT 1,

  `offered`         SMALLINT NOT NULL DEFAULT 0 COMMENT 'Contacts queued',
  `answered`        SMALLINT NOT NULL DEFAULT 0 COMMENT 'Contacts handled incoming',
  `abandoned`       SMALLINT NOT NULL DEFAULT 0 COMMENT 'Contacts abandoned',

  `ans_in_20`       SMALLINT NOT NULL DEFAULT 0,
  `ans_in_30`       SMALLINT NOT NULL DEFAULT 0,
  `ans_in_40`       SMALLINT NOT NULL DEFAULT 0,
  `ans_in_45`       SMALLINT NOT NULL DEFAULT 0,
  `ans_in_60`       SMALLINT NOT NULL DEFAULT 0,
  `ans_in_180`      SMALLINT NOT NULL DEFAULT 0,

  `abd_in_20`       SMALLINT NOT NULL DEFAULT 0,
  `abd_in_30`       SMALLINT NOT NULL DEFAULT 0,
  `abd_in_40`       SMALLINT NOT NULL DEFAULT 0,
  `abd_in_45`       SMALLINT NOT NULL DEFAULT 0,
  `abd_in_60`       SMALLINT NOT NULL DEFAULT 0,
  `abd_in_180`      SMALLINT NOT NULL DEFAULT 0,

  `ans_in_sla`      SMALLINT NOT NULL DEFAULT 0 COMMENT 'Answered within threshold',
  `ans_out_sla`     SMALLINT NOT NULL DEFAULT 0 COMMENT 'Answered but too slow',
  `abd_in_sla`      SMALLINT NOT NULL DEFAULT 0 COMMENT 'Abandoned within threshold (short abandons)',
  `abd_out_sla`     SMALLINT NOT NULL DEFAULT 0 COMMENT 'Abandoned after threshold (genuine losses)',

  `avg_handle_time` FLOAT DEFAULT NULL COMMENT 'AHT',
  `avg_answer_time` FLOAT DEFAULT NULL COMMENT 'ASA',
  `hold_time_total` INT   DEFAULT NULL COMMENT 'Customer hold time raw total (s)',
"""

TABLES['call_metrics_history'] = f"""
CREATE TABLE `call_metrics_history` (
{_METRICS_COLS}
  UNIQUE KEY `uq_queue_interval` (`queue_id`, `interval_start`),
  INDEX `idx_hist_queue_dt` (`queue_id`, `interval_start`),
  INDEX `idx_hist_dt`       (`interval_start`),
  INDEX `idx_hist_is_bh`    (`is_bh`),
  FOREIGN KEY (`queue_id`) REFERENCES `queue_config` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

TABLES['call_metrics_today'] = f"""
CREATE TABLE `call_metrics_today` (
{_METRICS_COLS}
  INDEX `idx_today_queue_dt` (`queue_id`, `interval_start`),
  INDEX `idx_today_is_bh`    (`is_bh`),
  FOREIGN KEY (`queue_id`) REFERENCES `queue_config` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

TABLES['today_snapshot_log'] = """
CREATE TABLE `today_snapshot_log` (
  `id`            INT AUTO_INCREMENT PRIMARY KEY,
  `snapshot_date` DATE NOT NULL,
  `rows_copied`   INT  NOT NULL DEFAULT 0,
  `refreshed_at`  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_snap_date` (`snapshot_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

DROP_ORDER = [
    'call_metrics_today', 'call_metrics_history',
    'call_metrics',         
    'today_snapshot_log',
    'queue_config',
    'sla_thresholds',        
    'company_kpi_rules',     
    'queues',                 
    'companies',
]

CREATE_ORDER = [
    'companies', 'queue_config',
    'call_metrics_history', 'call_metrics_today',
    'today_snapshot_log',
]


def setup():
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
    except mysql.connector.Error as e:
        print(f"  Cannot connect to MySQL: {e}")
        return

    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
        f"DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    print(f"  Database `{DB_NAME}` ready.")
    cnx.database = DB_NAME

    print("    Dropping old tables …")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in DROP_ORDER:
        cursor.execute(f"DROP TABLE IF EXISTS `{t}`")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    for name in CREATE_ORDER:
        try:
            cursor.execute(TABLES[name])
            print(f"    ✔  {name}")
        except mysql.connector.Error as e:
            print(f"    ✗  {name}: {e}")

    cnx.commit()
    cursor.close()
    cnx.close()
    print(f"\n     Schema ready.")
    print(f"    1 → python etl_v5.py")
    print(f"    2 → python refresh_today.py")
    print(f"    3 → python api_v5.py\n")


if __name__ == '__main__':
    setup()