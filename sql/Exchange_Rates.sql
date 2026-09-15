CREATE DATABASE FOREIGN_RATES;

USE FOREIGN_RATES;

CREATE TABLE IF NOT EXISTS EXCHANGE_RATES (
    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
    base_currency           VARCHAR(100)      NOT NULL,
    target_currency         VARCHAR(100)      NOT NULL,
    rate                    NUMERIC(50, 10)  NOT NULL,
    rate_date               DATE            NOT NULL,
    time_last_update_unix   BIGINT          NOT NULL,
    time_last_update_utc    DATETIME     NOT NULL,
    inserted_at             DATETIME     NOT NULL DEFAULT now(),
    CONSTRAINT uq_rate_snapshot 
    UNIQUE (base_currency, target_currency, time_last_update_unix)
);

CREATE INDEX ix_exchange_rates_base_date
ON EXCHANGE_RATES (base_currency, rate_date);


CREATE TABLE IF NOT EXISTS PIPELINE_WATERMARK(
    pipeline_name       VARCHAR(100)    NOT NULL,
    base_currency       VARCHAR(3)      NOT NULL,
    last_loaded_unix    BIGINT,
    last_loaded_at      DATETIME,
    updated_at          DATETIME     NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, base_currency)
);