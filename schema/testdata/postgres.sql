CREATE TABLE "flights" ("id" bigserial NOT NULL PRIMARY KEY, "airline_id" bigint NOT NULL, "name" varchar(100) NOT NULL, "code" char(3) NOT NULL, "price" decimal(8, 2) NOT NULL DEFAULT 0, "active" boolean NOT NULL DEFAULT TRUE, "status" varchar(255) NOT NULL CONSTRAINT "flights_status_check" CHECK ("status" IN ('scheduled', 'departed')), "meta" json NULL, "ref" uuid NOT NULL, "departs_at" timestamp(0) with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP, "seats" integer NOT NULL, "price_cents" integer GENERATED ALWAYS AS (price * 100) STORED, "notes" text NOT NULL, "summary" text NULL, "created_at" timestamp(0) without time zone NULL, "updated_at" timestamp(0) without time zone NULL, "deleted_at" timestamp(0) without time zone NULL, CONSTRAINT "flights_airline_id_foreign" FOREIGN KEY ("airline_id") REFERENCES "airlines" ("id") ON DELETE CASCADE)
CREATE UNIQUE INDEX "flights_code_unique" ON "flights" ("code")
CREATE INDEX "flights_ref_index" ON "flights" ("ref")
CREATE INDEX "flights_airline_departs" ON "flights" USING btree ("airline_id", "departs_at")
COMMENT ON COLUMN "flights"."name" IS 'flight name'
ALTER TABLE "flights" DROP CONSTRAINT "flights_airline_id_foreign"
ALTER TABLE "flights" ADD COLUMN "gate" varchar(10) NULL
ALTER TABLE "flights" DROP CONSTRAINT IF EXISTS "flights_name_check"
ALTER TABLE "flights" ALTER COLUMN "name" DROP DEFAULT
ALTER TABLE "flights" ALTER COLUMN "name" TYPE varchar(200) USING "name"::varchar(200)
ALTER TABLE "flights" ALTER COLUMN "name" SET NOT NULL
ALTER TABLE "flights" DROP CONSTRAINT IF EXISTS "flights_airline_id_check"
ALTER TABLE "flights" ALTER COLUMN "airline_id" DROP DEFAULT
ALTER TABLE "flights" ALTER COLUMN "airline_id" TYPE bigint USING "airline_id"::bigint
ALTER TABLE "flights" ALTER COLUMN "airline_id" DROP NOT NULL
ALTER TABLE "flights" RENAME COLUMN "notes" TO "remarks"
DO $storm$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = '"flights"'::regclass AND conname = 'flights_notes_check' AND contype = 'c') THEN ALTER TABLE "flights" RENAME CONSTRAINT "flights_notes_check" TO "flights_remarks_check"; END IF; END $storm$
ALTER TABLE "flights" DROP COLUMN "ref"
ALTER INDEX "flights_code_unique" RENAME TO "flights_code_uq"
ALTER TABLE "flights" ADD CONSTRAINT "flights_airline_id_foreign" FOREIGN KEY ("airline_id") REFERENCES "carriers" ("id") ON DELETE SET NULL
CREATE INDEX IF NOT EXISTS "flights_airline_id_index" ON "flights" ("airline_id")
