
CREATE DATABASE acn_registro
  WITH OWNER luca
       ENCODING 'UTF8'
       TEMPLATE template0;

-- =========================================================
-- ACN Registry (NIS2) - PostgreSQL 16 DDL + demo data
-- Re-runnable: droppa/ricrea tipi, tabelle, viste e trigger
-- =========================================================

-- (opzionale, solo per la sessione)
SET client_encoding = 'UTF8';
SET TIME ZONE 'UTC';

-- ---------- DROP idempotente ----------
DROP VIEW  IF EXISTS acn_dipendenze, acn_responsabili, acn_servizi, acn_asset;
DROP TABLE IF EXISTS responsabilita_hist, servizio_hist, asset_hist CASCADE;
DROP TABLE IF EXISTS dipendenza_servizio, responsabilita, ruolo, soggetto,
                     contratto, fornitore, ubicazione,
                     servizio, asset CASCADE;
DROP TYPE  IF EXISTS raci_enum, criticita_level, categoria_asset,
                     ambiente_enum, bersaglio_tipo, tipo_dep;

-- ---------- Estensioni utili ----------
CREATE EXTENSION IF NOT EXISTS citext;  -- per email case-insensitive

-- ---------- Enum ----------
CREATE TYPE criticita_level AS ENUM ('BASSA','MEDIA','ALTA','CRITICA');
CREATE TYPE categoria_asset AS ENUM ('HW','SW','RETE','DATI','ALTRO');
CREATE TYPE ambiente_enum   AS ENUM ('PROD','TEST','DEV');
CREATE TYPE bersaglio_tipo  AS ENUM ('SERVIZIO','ASSET');
CREATE TYPE tipo_dep        AS ENUM ('RUNTIME','DATI','RETE','TERZA_PARTE','ALTRO');
CREATE TYPE raci_enum       AS ENUM ('R','A','C','I');

-- ---------- 1) Inventario ----------
CREATE TABLE ubicazione (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sede       VARCHAR(120) NOT NULL,
  indirizzo  VARCHAR(255),
  note       VARCHAR(255)
);

CREATE TABLE fornitore (
  id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome  VARCHAR(160) NOT NULL,
  piva  VARCHAR(32),
  CONSTRAINT uq_fornitore_nome UNIQUE (nome)
);

CREATE TABLE contratto (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fornitore_id  BIGINT NOT NULL,
  codice        VARCHAR(64) NOT NULL,
  scadenza      DATE,
  sla           VARCHAR(160),
  CONSTRAINT uq_contratto UNIQUE (fornitore_id, codice),
  CONSTRAINT fk_contratto_fornitore
    FOREIGN KEY (fornitore_id) REFERENCES fornitore(id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE servizio (
  id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  service_code     VARCHAR(64)  NOT NULL,
  nome             VARCHAR(160) NOT NULL,
  descrizione      TEXT,
  owner_org        VARCHAR(160),
  criticita        criticita_level DEFAULT 'MEDIA',
  dominio_business VARCHAR(120),
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_servizio_code UNIQUE (service_code)
);

CREATE TABLE asset (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_code     VARCHAR(64)  NOT NULL,
  nome           VARCHAR(160) NOT NULL,
  categoria      categoria_asset NOT NULL DEFAULT 'ALTRO',
  criticita      criticita_level DEFAULT 'MEDIA',
  ambiente       ambiente_enum   DEFAULT 'PROD',
  ubicazione_id  BIGINT,
  fornitore_id   BIGINT,
  versione       VARCHAR(80),
  stato          VARCHAR(80),
  contratto_id   BIGINT,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_asset_code UNIQUE (asset_code),
  CONSTRAINT fk_asset_ubicazione
    FOREIGN KEY (ubicazione_id) REFERENCES ubicazione(id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_asset_fornitore
    FOREIGN KEY (fornitore_id) REFERENCES fornitore(id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_asset_contratto
    FOREIGN KEY (contratto_id) REFERENCES contratto(id)
    ON DELETE SET NULL ON UPDATE CASCADE
);
CREATE INDEX idx_asset_ubicazione ON asset(ubicazione_id);
CREATE INDEX idx_asset_fornitore  ON asset(fornitore_id);
CREATE INDEX idx_asset_contratto  ON asset(contratto_id);

-- ---------- 2) Responsabilità (RACI) ----------
CREATE TABLE soggetto (
  id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome     VARCHAR(160) NOT NULL,
  email    CITEXT,                 -- case-insensitive
  telefono VARCHAR(40),
  CONSTRAINT uq_soggetto_email UNIQUE (email)
);

CREATE TABLE ruolo (
  id     SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codice VARCHAR(64)  NOT NULL,
  nome   VARCHAR(120) NOT NULL,
  CONSTRAINT uq_ruolo_codice UNIQUE (codice)
);

CREATE TABLE responsabilita (
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  soggetto_id          BIGINT    NOT NULL,
  ruolo_id             SMALLINT  NOT NULL,
  servizio_id          BIGINT,
  asset_id             BIGINT,
  tipo_responsabilita  raci_enum NOT NULL,
  note                 VARCHAR(255),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_resp_soggetto  FOREIGN KEY (soggetto_id) REFERENCES soggetto(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_resp_ruolo     FOREIGN KEY (ruolo_id)    REFERENCES ruolo(id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_resp_servizio  FOREIGN KEY (servizio_id) REFERENCES servizio(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_resp_asset     FOREIGN KEY (asset_id)    REFERENCES asset(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  -- esattamente uno tra servizio_id e asset_id
  CONSTRAINT ck_responsabilita_target_xor
    CHECK ((servizio_id IS NULL) <> (asset_id IS NULL))
);
CREATE INDEX idx_resp_serv  ON responsabilita(servizio_id);
CREATE INDEX idx_resp_asset ON responsabilita(asset_id);

-- Una (e una sola) 'A' per servizio: indice univoco parziale
CREATE UNIQUE INDEX uq_responsabilita_single_A
  ON responsabilita(servizio_id)
  WHERE (tipo_responsabilita = 'A');

-- ---------- 3) Dipendenze (grafo orientato) ----------
CREATE TABLE dipendenza_servizio (
  id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fonte_servizio_id  BIGINT NOT NULL,
  bersaglio_tipo     bersaglio_tipo NOT NULL,
  bersaglio_id       BIGINT NOT NULL,
  tipo               tipo_dep       DEFAULT 'ALTRO',
  criticita_impatti  criticita_level DEFAULT 'MEDIA',
  note               VARCHAR(255),
  created_at         timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT fk_dep_fonte_servizio
    FOREIGN KEY (fonte_servizio_id) REFERENCES servizio(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT uq_dep UNIQUE (fonte_servizio_id, bersaglio_tipo, bersaglio_id, tipo)
);
CREATE INDEX idx_dep_bersaglio ON dipendenza_servizio(bersaglio_tipo, bersaglio_id);

-- Trigger anti-cicli (solo quando il bersaglio è un SERVIZIO)
CREATE OR REPLACE FUNCTION trg_no_cycles_dep() RETURNS trigger AS $$
DECLARE hit INT;
BEGIN
  IF NEW.bersaglio_tipo = 'SERVIZIO' THEN
    WITH RECURSIVE reach(n) AS (
      SELECT NEW.bersaglio_id
      UNION ALL
      SELECT d.fonte_servizio_id
      FROM dipendenza_servizio d
      JOIN reach r
        ON d.bersaglio_tipo = 'SERVIZIO'
       AND d.bersaglio_id   = r.n
    )
    SELECT 1 INTO hit FROM reach WHERE n = NEW.fonte_servizio_id LIMIT 1;

    IF FOUND THEN
      RAISE EXCEPTION 'Ciclo di dipendenze non consentito (%, -> %)', NEW.fonte_servizio_id, NEW.bersaglio_id
        USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_dep_no_cycles_bi
BEFORE INSERT ON dipendenza_servizio
FOR EACH ROW EXECUTE FUNCTION trg_no_cycles_dep();

-- ---------- Timestamps: aggiorna updated_at automatico ----------
CREATE OR REPLACE FUNCTION trg_set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_upd_ts_servizio
BEFORE UPDATE ON servizio
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_upd_ts_asset
BEFORE UPDATE ON asset
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_upd_ts_responsabilita
BEFORE UPDATE ON responsabilita
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

-- ---------- 4) Storico / Versioning (SCD2 minimal) ----------
-- Tabelle *_hist: stesse colonne principali + valid_from/valid_to
CREATE TABLE servizio_hist (
  id               BIGINT NOT NULL,
  service_code     VARCHAR(64)  NOT NULL,
  nome             VARCHAR(160) NOT NULL,
  descrizione      TEXT,
  owner_org        VARCHAR(160),
  criticita        criticita_level,
  dominio_business VARCHAR(120),
  created_at       timestamptz NOT NULL,
  updated_at       timestamptz NOT NULL,
  valid_from       timestamptz NOT NULL,
  valid_to         timestamptz
);

CREATE TABLE asset_hist (
  id             BIGINT NOT NULL,
  asset_code     VARCHAR(64)  NOT NULL,
  nome           VARCHAR(160) NOT NULL,
  categoria      categoria_asset,
  criticita      criticita_level,
  ambiente       ambiente_enum,
  ubicazione_id  BIGINT,
  fornitore_id   BIGINT,
  versione       VARCHAR(80),
  stato          VARCHAR(80),
  contratto_id   BIGINT,
  created_at     timestamptz NOT NULL,
  updated_at     timestamptz NOT NULL,
  valid_from     timestamptz NOT NULL,
  valid_to       timestamptz
);

CREATE TABLE responsabilita_hist (
  id                   BIGINT NOT NULL,
  soggetto_id          BIGINT NOT NULL,
  ruolo_id             SMALLINT NOT NULL,
  servizio_id          BIGINT,
  asset_id             BIGINT,
  tipo_responsabilita  raci_enum NOT NULL,
  note                 VARCHAR(255),
  created_at           timestamptz NOT NULL,
  updated_at           timestamptz NOT NULL,
  valid_from           timestamptz NOT NULL,
  valid_to             timestamptz
);

-- Funzioni SCD2
CREATE OR REPLACE FUNCTION scd2_servizio_ins() RETURNS trigger AS $$
BEGIN
  INSERT INTO servizio_hist
  (id, service_code, nome, descrizione, owner_org, criticita, dominio_business,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.service_code, NEW.nome, NEW.descrizione, NEW.owner_org, NEW.criticita, NEW.dominio_business,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_servizio_upd() RETURNS trigger AS $$
BEGIN
  UPDATE servizio_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  INSERT INTO servizio_hist
  (id, service_code, nome, descrizione, owner_org, criticita, dominio_business,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.service_code, NEW.nome, NEW.descrizione, NEW.owner_org, NEW.criticita, NEW.dominio_business,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_servizio_del() RETURNS trigger AS $$
BEGIN
  UPDATE servizio_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  RETURN OLD;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER tr_servizio_ai AFTER INSERT ON servizio
FOR EACH ROW EXECUTE FUNCTION scd2_servizio_ins();

CREATE TRIGGER tr_servizio_au AFTER UPDATE ON servizio
FOR EACH ROW EXECUTE FUNCTION scd2_servizio_upd();

CREATE TRIGGER tr_servizio_bd BEFORE DELETE ON servizio
FOR EACH ROW EXECUTE FUNCTION scd2_servizio_del();

-- asset SCD2
CREATE OR REPLACE FUNCTION scd2_asset_ins() RETURNS trigger AS $$
BEGIN
  INSERT INTO asset_hist
  (id, asset_code, nome, categoria, criticita, ambiente, ubicazione_id, fornitore_id,
   versione, stato, contratto_id, created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.asset_code, NEW.nome, NEW.categoria, NEW.criticita, NEW.ambiente, NEW.ubicazione_id, NEW.fornitore_id,
   NEW.versione, NEW.stato, NEW.contratto_id, NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_asset_upd() RETURNS trigger AS $$
BEGIN
  UPDATE asset_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  INSERT INTO asset_hist
  (id, asset_code, nome, categoria, criticita, ambiente, ubicazione_id, fornitore_id,
   versione, stato, contratto_id, created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.asset_code, NEW.nome, NEW.categoria, NEW.criticita, NEW.ambiente, NEW.ubicazione_id, NEW.fornitore_id,
   NEW.versione, NEW.stato, NEW.contratto_id, NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_asset_del() RETURNS trigger AS $$
BEGIN
  UPDATE asset_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  RETURN OLD;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER tr_asset_ai AFTER INSERT ON asset
FOR EACH ROW EXECUTE FUNCTION scd2_asset_ins();

CREATE TRIGGER tr_asset_au AFTER UPDATE ON asset
FOR EACH ROW EXECUTE FUNCTION scd2_asset_upd();

CREATE TRIGGER tr_asset_bd BEFORE DELETE ON asset
FOR EACH ROW EXECUTE FUNCTION scd2_asset_del();

-- responsabilita SCD2
CREATE OR REPLACE FUNCTION scd2_resp_ins() RETURNS trigger AS $$
BEGIN
  INSERT INTO responsabilita_hist
  (id, soggetto_id, ruolo_id, servizio_id, asset_id, tipo_responsabilita,
   note, created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.soggetto_id, NEW.ruolo_id, NEW.servizio_id, NEW.asset_id, NEW.tipo_responsabilita,
   NEW.note, NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_resp_upd() RETURNS trigger AS $$
BEGIN
  UPDATE responsabilita_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  INSERT INTO responsabilita_hist
  (id, soggetto_id, ruolo_id, servizio_id, asset_id, tipo_responsabilita,
   note, created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.soggetto_id, NEW.ruolo_id, NEW.servizio_id, NEW.asset_id, NEW.tipo_responsabilita,
   NEW.note, NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_resp_del() RETURNS trigger AS $$
BEGIN
  UPDATE responsabilita_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  RETURN OLD;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER tr_resp_ai AFTER INSERT ON responsabilita
FOR EACH ROW EXECUTE FUNCTION scd2_resp_ins();

CREATE TRIGGER tr_resp_au AFTER UPDATE ON responsabilita
FOR EACH ROW EXECUTE FUNCTION scd2_resp_upd();

CREATE TRIGGER tr_resp_bd BEFORE DELETE ON responsabilita
FOR EACH ROW EXECUTE FUNCTION scd2_resp_del();

-- ---------- 5) Visite "ACN" ----------
CREATE VIEW acn_asset AS
SELECT a.asset_code   AS codice,
       a.nome         AS nome,
       a.categoria    AS categoria,
       a.criticita    AS criticita,
       u.sede         AS sede,
       f.nome         AS fornitore
FROM asset a
LEFT JOIN ubicazione u ON u.id = a.ubicazione_id
LEFT JOIN fornitore f  ON f.id = a.fornitore_id;

CREATE VIEW acn_servizi AS
SELECT s.service_code AS codice,
       s.nome         AS nome,
       s.criticita    AS criticita,
       so.email       AS owner_email
FROM servizio s
LEFT JOIN responsabilita r
  ON r.servizio_id = s.id AND r.tipo_responsabilita = 'A'
LEFT JOIN soggetto so ON so.id = r.soggetto_id;

CREATE VIEW acn_responsabili AS
SELECT r.id,
       COALESCE(sv.service_code, a.asset_code) AS target_codice,
       CASE WHEN r.servizio_id IS NOT NULL THEN 'SERVIZIO' ELSE 'ASSET' END AS target_tipo,
       sj.nome   AS soggetto,
       rl.codice AS ruolo,
       r.tipo_responsabilita AS raci
FROM responsabilita r
LEFT JOIN servizio sv ON sv.id = r.servizio_id
LEFT JOIN asset    a  ON a.id  = r.asset_id
JOIN soggetto sj    ON sj.id = r.soggetto_id
JOIN ruolo rl       ON rl.id = r.ruolo_id;

CREATE VIEW acn_dipendenze AS
SELECT d.id,
       sf.service_code AS servizio_fonte,
       d.bersaglio_tipo,
       CASE
         WHEN d.bersaglio_tipo = 'SERVIZIO' THEN sb.service_code
         ELSE a.asset_code
       END AS bersaglio_codice,
       d.tipo, d.criticita_impatti
FROM dipendenza_servizio d
JOIN servizio sf ON sf.id = d.fonte_servizio_id
LEFT JOIN servizio sb ON sb.id = d.bersaglio_id AND d.bersaglio_tipo = 'SERVIZIO'
LEFT JOIN asset    a  ON a.id  = d.bersaglio_id AND d.bersaglio_tipo = 'ASSET';

-- ---------- 6) Dati di esempio ----------
INSERT INTO ubicazione (sede, indirizzo) VALUES
  ('Sede Centrale', 'Via Roma 1, Milano'),
  ('Datacenter A',  'Viale Europa 100, Milano');

INSERT INTO fornitore (nome, piva) VALUES
  ('Acme Cloud', 'IT01234567890'),
  ('Rete&Switch Srl', 'IT10293847566');

INSERT INTO contratto (fornitore_id, codice, scadenza, sla) VALUES
  (1, 'CLOUD-2025', '2025-12-31', '99.9% uptime'),
  (2, 'NET-2026',   '2026-06-30', '4h fix');

INSERT INTO servizio (service_code, nome, descrizione, owner_org, criticita, dominio_business)
VALUES
  ('SVC-PORTALE', 'Portale Clienti', 'Frontend pubblico clienti', 'IT-APP', 'ALTA',   'Vendite'),
  ('SVC-API',     'API Backend',     'Servizi applicativi',       'IT-APP', 'CRITICA','Vendite');

INSERT INTO asset (asset_code, nome, categoria, criticita, ambiente, ubicazione_id, fornitore_id, versione, stato, contratto_id)
VALUES
  ('VM-WEB-01', 'VM Web 01', 'HW',   'ALTA',   'PROD', 2, 1, 'v23.10', 'In esercizio', 1),
  ('DB-01',     'Database Prod', 'DATI','CRITICA','PROD', 2, 1, '14.10', 'In esercizio', 1),
  ('FW-EDGE',   'Firewall Perimetrale', 'RETE','CRITICA','PROD', 2, 2, NULL,  'In esercizio', 2);

INSERT INTO soggetto (nome, email, telefono) VALUES
  ('Mario Rossi',  'mario.rossi@example.org', '+39-02-0001'),
  ('Laura Bianchi','laura.bianchi@example.org', '+39-02-0002'),
  ('DPO Team',     'dpo@example.org', NULL);

INSERT INTO ruolo (codice, nome) VALUES
  ('SERVICE_OWNER','Service Owner'),
  ('TECNICO',      'Tecnico'),
  ('RESP_SEC',     'Responsabile Sicurezza'),
  ('DPO',          'Data Protection Officer');

-- Responsabilità: una sola 'A' per servizio (vincolo via indice parziale)
INSERT INTO responsabilita (soggetto_id, ruolo_id, servizio_id, asset_id, tipo_responsabilita, note)
VALUES
  (1, 1, 1, NULL, 'A', 'Owner Portale'),
  (2, 2, 1, NULL, 'R', 'Tecnico Portale'),
  (3, 4, NULL, 2, 'C', 'Consulenza privacy su DB');

-- Dipendenze: Portale -> API (runtime), API -> DB (dati), Portale -> FW (rete)
INSERT INTO dipendenza_servizio (fonte_servizio_id, bersaglio_tipo, bersaglio_id, tipo, criticita_impatti)
VALUES
  (1, 'SERVIZIO', 2, 'RUNTIME', 'ALTA'),
  (2, 'ASSET',    2, 'DATI',    'CRITICA'),
  (1, 'ASSET',    3, 'RETE',    'MEDIA');
