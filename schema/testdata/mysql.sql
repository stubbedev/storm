CREATE TABLE `flights` (`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `airline_id` bigint unsigned NOT NULL, `name` varchar(100) NOT NULL COMMENT 'flight name', `code` char(3) NOT NULL, `price` decimal(8, 2) NOT NULL DEFAULT 0, `active` tinyint(1) NOT NULL DEFAULT 1, `status` enum('scheduled', 'departed') NOT NULL, `meta` json NULL, `ref` char(36) NOT NULL, `departs_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `seats` int unsigned NOT NULL, `price_cents` int AS (price * 100) STORED, `notes` text NOT NULL, `summary` text NULL, `created_at` timestamp NULL, `updated_at` timestamp NULL, `deleted_at` timestamp NULL, CONSTRAINT `flights_airline_id_foreign` FOREIGN KEY (`airline_id`) REFERENCES `airlines` (`id`) ON DELETE CASCADE, UNIQUE KEY `flights_code_unique` (`code`), INDEX `flights_ref_index` (`ref`), INDEX `flights_airline_departs` (`airline_id`, `departs_at`) USING BTREE)
ALTER TABLE `flights` DROP FOREIGN KEY `flights_airline_id_foreign`
ALTER TABLE `flights` ADD COLUMN `gate` varchar(10) NULL
ALTER TABLE `flights` MODIFY `name` varchar(200) NOT NULL
ALTER TABLE `flights` MODIFY `airline_id` bigint unsigned NULL
ALTER TABLE `flights` RENAME COLUMN `notes` TO `remarks`
ALTER TABLE `flights` DROP COLUMN `ref`
ALTER TABLE `flights` RENAME INDEX `flights_code_unique` TO `flights_code_uq`
ALTER TABLE `flights` ADD CONSTRAINT `flights_airline_id_foreign` FOREIGN KEY (`airline_id`) REFERENCES `carriers` (`id`) ON DELETE SET NULL
