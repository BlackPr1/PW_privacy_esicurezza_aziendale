CREATE DATABASE acn_registro
  WITH OWNER luca
       ENCODING 'UTF8'
       TEMPLATE template0;

-- (opzionale, solo per la sessione)
SET client_encoding = 'UTF8';
SET TIME ZONE 'UTC';

-- ---------- DROP idempotente ----------
DROP VIEW  IF EXISTS acn_asset_controlli, acn_gap, acn_profilo,
                     acn_dipendenze, acn_responsabili, acn_servizi, acn_asset;

DROP TABLE IF EXISTS asset_controllo_hist, profilo_subcategoria_hist CASCADE;

DROP TABLE IF EXISTS asset_controllo, controllo_subcategoria,
                     profilo_subcategoria, profilo,
                     controllo, subcategoria CASCADE;

DROP TABLE IF EXISTS responsabilita_hist, servizio_hist, asset_hist CASCADE;
DROP TABLE IF EXISTS dipendenza_servizio, responsabilita, ruolo, soggetto,
                     contratto, fornitore, ubicazione,
                     servizio, asset CASCADE;

DROP DOMAIN IF EXISTS maturita_livello;
DROP TYPE  IF EXISTS profilo_tipo, raci_enum, criticita_level, categoria_asset,
                     ambiente_enum, bersaglio_tipo, tipo_dep;

-- ---------- Estensioni utili ----------
CREATE EXTENSION IF NOT EXISTS citext;  -- per email case-insensitive

-- ---------- Enum / Domain ----------
CREATE TYPE criticita_level AS ENUM ('BASSA','MEDIA','ALTA','CRITICA');
CREATE TYPE categoria_asset AS ENUM ('HW','SW','RETE','DATI','ALTRO');
CREATE TYPE ambiente_enum   AS ENUM ('PROD','TEST','DEV');
CREATE TYPE bersaglio_tipo  AS ENUM ('SERVIZIO','ASSET');
CREATE TYPE tipo_dep        AS ENUM ('RUNTIME','DATI','RETE','TERZA_PARTE','ALTRO');
CREATE TYPE raci_enum       AS ENUM ('R','A','C','I');

-- Profilo attuale / target (per il "Framework Nazionale" in stile CSF)
CREATE TYPE profilo_tipo    AS ENUM ('ATTUALE','TARGET');

-- Livelli numerici di maturità (0..4)
CREATE DOMAIN maturita_livello AS SMALLINT
  CHECK (VALUE BETWEEN 0 AND 4);

-- =========================================================
-- 1) Inventario (asset, servizi, fornitori, ubicazioni)
-- =========================================================
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

-- =========================================================
-- 2) Responsabilità (RACI)
-- =========================================================
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

-- =========================================================
-- 3) Dipendenze (grafo orientato) - servizi → (servizi|asset)
-- =========================================================
CREATE TABLE dipendenza_servizio (
  id                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fonte_servizio_id  BIGINT NOT NULL,
  bersaglio_tipo     bersaglio_tipo NOT NULL,
  bersaglio_id       BIGINT NOT NULL,
  tipo               tipo_dep        DEFAULT 'ALTRO',
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

-- =========================================================
-- 4) Timestamps: aggiorna updated_at automatico
-- =========================================================
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

-- =========================================================
-- 5) Storico / Versioning (SCD2 minimal) per servizio/asset/responsabilita
-- =========================================================
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

-- Funzioni SCD2: servizio
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

-- Funzioni SCD2: asset
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

-- Funzioni SCD2: responsabilita
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

-- =========================================================
-- 6) Profilo (Framework Nazionale): subcategory + controlli + livelli
-- =========================================================

-- Subcategory (funzione/categoria/subcategory) del framework
CREATE TABLE subcategoria (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codice      VARCHAR(32)  NOT NULL,         -- es: ID.AM-1
  funzione    VARCHAR(40)  NOT NULL,         -- es: IDENTIFY / PROTECT ...
  categoria   VARCHAR(80)  NOT NULL,         -- es: ID.AM
  descrizione TEXT         NOT NULL,
  riferimento TEXT,                          -- link o riferimento normativo/linee guida
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_subcategoria_codice UNIQUE (codice)
);

-- Controlli interni (misure/contromisure)
CREATE TABLE controllo (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  control_code    VARCHAR(64)  NOT NULL,     -- codice interno: CTRL-...
  titolo          VARCHAR(160) NOT NULL,
  descrizione     TEXT,
  owner_ruolo_id  SMALLINT,
  owner_soggetto_id BIGINT,
  periodicita     VARCHAR(80),               -- es: trimestrale, annuale...
  evidenza_attesa TEXT,                      -- che tipo di evidenze si allegano
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_controllo_code UNIQUE (control_code),
  CONSTRAINT fk_controllo_ruolo
    FOREIGN KEY (owner_ruolo_id) REFERENCES ruolo(id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_controllo_soggetto
    FOREIGN KEY (owner_soggetto_id) REFERENCES soggetto(id)
    ON DELETE SET NULL ON UPDATE CASCADE
);

-- Mappa controllo ↔ subcategory (molti-a-molti)
CREATE TABLE controllo_subcategoria (
  controllo_id   BIGINT NOT NULL,
  subcategoria_id BIGINT NOT NULL,
  note           VARCHAR(255),
  PRIMARY KEY (controllo_id, subcategoria_id),
  CONSTRAINT fk_cs_controllo
    FOREIGN KEY (controllo_id) REFERENCES controllo(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_cs_subcategoria
    FOREIGN KEY (subcategoria_id) REFERENCES subcategoria(id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

-- Profili (attuale/target). Non contiene "fncs" nel nome, ma risponde al requisito.
CREATE TABLE profilo (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nome        VARCHAR(160) NOT NULL,         -- es: "Profilo 2026"
  tipo        profilo_tipo NOT NULL,
  descrizione TEXT,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_profilo UNIQUE (nome, tipo)
);

-- Livello di maturità per subcategory nel profilo (0..4)
CREATE TABLE profilo_subcategoria (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  profilo_id     BIGINT NOT NULL,
  subcategoria_id BIGINT NOT NULL,
  livello        maturita_livello NOT NULL,
  motivazione    TEXT,
  evidenza       TEXT,
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_profilo_subc UNIQUE (profilo_id, subcategoria_id),
  CONSTRAINT fk_ps_profilo
    FOREIGN KEY (profilo_id) REFERENCES profilo(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_ps_subcategoria
    FOREIGN KEY (subcategoria_id) REFERENCES subcategoria(id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

-- Associazione asset → controllo + valutazione (0..4)
CREATE TABLE asset_controllo (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  asset_id         BIGINT NOT NULL,
  controllo_id     BIGINT NOT NULL,
  livello          maturita_livello NOT NULL,
  assessed_at      timestamptz NOT NULL DEFAULT now(),
  assessor_soggetto_id BIGINT,
  evidenza         TEXT,
  note             TEXT,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_asset_controllo UNIQUE (asset_id, controllo_id),
  CONSTRAINT fk_ac_asset
    FOREIGN KEY (asset_id) REFERENCES asset(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_ac_controllo
    FOREIGN KEY (controllo_id) REFERENCES controllo(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_ac_assessor
    FOREIGN KEY (assessor_soggetto_id) REFERENCES soggetto(id)
    ON DELETE SET NULL ON UPDATE CASCADE
);
CREATE INDEX idx_ac_controllo ON asset_controllo(controllo_id);

-- Trigger updated_at per le nuove tabelle
CREATE TRIGGER tr_upd_ts_subcategoria
BEFORE UPDATE ON subcategoria
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_upd_ts_controllo
BEFORE UPDATE ON controllo
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_upd_ts_profilo
BEFORE UPDATE ON profilo
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_upd_ts_profilo_subcategoria
BEFORE UPDATE ON profilo_subcategoria
FOR EACH ROW EXECUTE FUNCTION trg_set_updated_at();

-- Per asset_controllo: oltre a updated_at aggiorna anche assessed_at
CREATE OR REPLACE FUNCTION trg_set_assessed_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at  := now();
  NEW.assessed_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER tr_upd_ts_asset_controllo
BEFORE UPDATE ON asset_controllo
FOR EACH ROW EXECUTE FUNCTION trg_set_assessed_at();

-- =========================================================
-- 7) Storico / Versioning (SCD2) per profilo_subcategoria e asset_controllo
-- =========================================================
CREATE TABLE profilo_subcategoria_hist (
  id             BIGINT NOT NULL,
  profilo_id     BIGINT NOT NULL,
  subcategoria_id BIGINT NOT NULL,
  livello        maturita_livello NOT NULL,
  motivazione    TEXT,
  evidenza       TEXT,
  created_at     timestamptz NOT NULL,
  updated_at     timestamptz NOT NULL,
  valid_from     timestamptz NOT NULL,
  valid_to       timestamptz
);

CREATE TABLE asset_controllo_hist (
  id              BIGINT NOT NULL,
  asset_id         BIGINT NOT NULL,
  controllo_id     BIGINT NOT NULL,
  livello          maturita_livello NOT NULL,
  assessed_at      timestamptz NOT NULL,
  assessor_soggetto_id BIGINT,
  evidenza         TEXT,
  note             TEXT,
  created_at       timestamptz NOT NULL,
  updated_at       timestamptz NOT NULL,
  valid_from       timestamptz NOT NULL,
  valid_to         timestamptz
);

-- SCD2 profilo_subcategoria
CREATE OR REPLACE FUNCTION scd2_ps_ins() RETURNS trigger AS $$
BEGIN
  INSERT INTO profilo_subcategoria_hist
  (id, profilo_id, subcategoria_id, livello, motivazione, evidenza,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.profilo_id, NEW.subcategoria_id, NEW.livello, NEW.motivazione, NEW.evidenza,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_ps_upd() RETURNS trigger AS $$
BEGIN
  UPDATE profilo_subcategoria_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  INSERT INTO profilo_subcategoria_hist
  (id, profilo_id, subcategoria_id, livello, motivazione, evidenza,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.profilo_id, NEW.subcategoria_id, NEW.livello, NEW.motivazione, NEW.evidenza,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_ps_del() RETURNS trigger AS $$
BEGIN
  UPDATE profilo_subcategoria_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  RETURN OLD;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER tr_ps_ai AFTER INSERT ON profilo_subcategoria
FOR EACH ROW EXECUTE FUNCTION scd2_ps_ins();

CREATE TRIGGER tr_ps_au AFTER UPDATE ON profilo_subcategoria
FOR EACH ROW EXECUTE FUNCTION scd2_ps_upd();

CREATE TRIGGER tr_ps_bd BEFORE DELETE ON profilo_subcategoria
FOR EACH ROW EXECUTE FUNCTION scd2_ps_del();

-- SCD2 asset_controllo
CREATE OR REPLACE FUNCTION scd2_ac_ins() RETURNS trigger AS $$
BEGIN
  INSERT INTO asset_controllo_hist
  (id, asset_id, controllo_id, livello, assessed_at, assessor_soggetto_id, evidenza, note,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.asset_id, NEW.controllo_id, NEW.livello, NEW.assessed_at, NEW.assessor_soggetto_id, NEW.evidenza, NEW.note,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_ac_upd() RETURNS trigger AS $$
BEGIN
  UPDATE asset_controllo_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  INSERT INTO asset_controllo_hist
  (id, asset_id, controllo_id, livello, assessed_at, assessor_soggetto_id, evidenza, note,
   created_at, updated_at, valid_from, valid_to)
  VALUES
  (NEW.id, NEW.asset_id, NEW.controllo_id, NEW.livello, NEW.assessed_at, NEW.assessor_soggetto_id, NEW.evidenza, NEW.note,
   NEW.created_at, NEW.updated_at, now(), NULL);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION scd2_ac_del() RETURNS trigger AS $$
BEGIN
  UPDATE asset_controllo_hist
     SET valid_to = now()
   WHERE id = OLD.id AND valid_to IS NULL;
  RETURN OLD;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER tr_ac_ai AFTER INSERT ON asset_controllo
FOR EACH ROW EXECUTE FUNCTION scd2_ac_ins();

CREATE TRIGGER tr_ac_au AFTER UPDATE ON asset_controllo
FOR EACH ROW EXECUTE FUNCTION scd2_ac_upd();

CREATE TRIGGER tr_ac_bd BEFORE DELETE ON asset_controllo
FOR EACH ROW EXECUTE FUNCTION scd2_ac_del();

-- =========================================================
-- 8) Viste "ACN" (come nello schema originale) + viste profilo/controlli
-- =========================================================
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

-- Vista profilo (righe: profilo + subcategory + livello)
CREATE VIEW acn_profilo AS
SELECT p.nome        AS profilo_nome,
       p.tipo        AS profilo_tipo,
       sc.codice     AS subcategory_codice,
       sc.funzione   AS funzione,
       sc.categoria  AS categoria,
       ps.livello    AS livello,
       ps.motivazione,
       ps.evidenza
FROM profilo_subcategoria ps
JOIN profilo p      ON p.id  = ps.profilo_id
JOIN subcategoria sc ON sc.id = ps.subcategoria_id;

-- Vista gap (target vs attuale) sulle subcategory
CREATE VIEW acn_gap AS
SELECT sc.codice    AS subcategory_codice,
       sc.funzione  AS funzione,
       sc.categoria AS categoria,
       cur.livello  AS livello_attuale,
       tgt.livello  AS livello_target,
       (tgt.livello - cur.livello)::INT AS gap
FROM subcategoria sc
JOIN profilo_subcategoria cur ON cur.subcategoria_id = sc.id
JOIN profilo pc ON pc.id = cur.profilo_id AND pc.tipo = 'ATTUALE'
JOIN profilo_subcategoria tgt ON tgt.subcategoria_id = sc.id
JOIN profilo pt ON pt.id = tgt.profilo_id AND pt.tipo = 'TARGET'
WHERE (tgt.livello > cur.livello);

-- Vista asset→controlli (utile per collegare controlli e subcategory agli asset)
CREATE VIEW acn_asset_controlli AS
SELECT a.asset_code,
       a.nome AS asset_nome,
       c.control_code,
       c.titolo AS controllo_titolo,
       ac.livello,
       ac.assessed_at,
       sc.codice AS subcategory_codice,
       sc.funzione,
       sc.categoria
FROM asset_controllo ac
JOIN asset a ON a.id = ac.asset_id
JOIN controllo c ON c.id = ac.controllo_id
LEFT JOIN controllo_subcategoria cs ON cs.controllo_id = c.id
LEFT JOIN subcategoria sc ON sc.id = cs.subcategoria_id;

-- =========================================================
-- 9) Dati di esempio (inventario + profilo/controlli)
-- =========================================================

-- Inventario base
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

-- --- Subcategory (esempi minimi, puoi estendere in base al framework adottato) ---
INSERT INTO subcategoria (codice, funzione, categoria, descrizione, riferimento) VALUES
  ('ID.AM-1','IDENTIFY','ID.AM','Inventario dei dispositivi e dei sistemi (asset) mantenuto aggiornato.','Framework Nazionale - Asset Management'),
  ('ID.AM-2','IDENTIFY','ID.AM','Inventario delle applicazioni e dei servizi mantenuto aggiornato.','Framework Nazionale - Asset Management'),
  ('PR.AC-1','PROTECT','PR.AC','Gestione delle identità e degli accessi (account, ruoli, privilegi).','Framework Nazionale - Access Control'),
  ('PR.DS-1','PROTECT','PR.DS','Protezione dei dati (classificazione, cifratura, backup).','Framework Nazionale - Data Security'),
  ('DE.CM-1','DETECT','DE.CM','Monitoraggio continuo per rilevare eventi anomali e incidenti.','Framework Nazionale - Monitoring'),
  ('RS.RP-1','RESPOND','RS.RP','Pianificazione e procedure di risposta agli incidenti.','Framework Nazionale - Response Planning');

-- --- Controlli (esempi) ---
INSERT INTO controllo (control_code, titolo, descrizione, owner_ruolo_id, periodicita, evidenza_attesa) VALUES
  ('CTRL-IAM-01','Gestione identità e privilegi','Processo di gestione account, ruoli e revisione privilegi.', 3, 'Trimestrale','Report revisione privilegi, elenco account, log approvazioni'),
  ('CTRL-BKP-01','Backup e restore periodico','Backup schedulati e test di ripristino su dati critici.', 2, 'Mensile','Log backup, report esito restore test'),
  ('CTRL-MON-01','Monitoraggio e alerting','Raccolta log e alerting su eventi critici.', 3, 'Continuo','Dashboard, regole alert, evidenze gestione falsi positivi'),
  ('CTRL-IR-01','Incident response','Playbook e procedure di gestione incidenti, con esercitazioni.', 3, 'Semestrale','Verbali tabletop, playbook aggiornato, lesson learned');

-- Mappa controlli→subcategory
INSERT INTO controllo_subcategoria (controllo_id, subcategoria_id) VALUES
  (1, 3), -- IAM -> PR.AC-1
  (2, 4), -- Backup -> PR.DS-1
  (3, 5), -- Monitoring -> DE.CM-1
  (4, 6); -- IR -> RS.RP-1

-- Profili (attuale e target)
INSERT INTO profilo (nome, tipo, descrizione) VALUES
  ('Profilo 2026', 'ATTUALE', 'Profilo corrente rilevato nel 2026'),
  ('Profilo 2026', 'TARGET',  'Profilo obiettivo (target) per roadmap 2026');

-- Livelli profilo (0..4) su alcune subcategory
-- ATTUALE (id=1) / TARGET (id=2)
INSERT INTO profilo_subcategoria (profilo_id, subcategoria_id, livello, motivazione) VALUES
  (1, 1, 2, 'Inventario asset presente ma processo non completamente formalizzato.'),
  (2, 1, 4, 'Inventario completo, owner definito, riconciliazione periodica.'),
  (1, 3, 1, 'IAM presente ma revisione privilegi discontinua.'),
  (2, 3, 3, 'Revisione privilegi trimestrale + tracciamento approvazioni.'),
  (1, 4, 2, 'Backup eseguiti, test restore non sempre tracciati.'),
  (2, 4, 4, 'Test restore regolari con evidenze e KPI.'),
  (1, 5, 1, 'Monitoraggio parziale, copertura log incompleta.'),
  (2, 5, 3, 'Copertura log estesa e tuning alert.'),
  (1, 6, 2, 'Playbook esistente, esercitazioni non regolari.'),
  (2, 6, 4, 'Esercitazioni semestrali e miglioramento continuo.');

-- Valutazioni asset→controlli (0..4) su esempi
INSERT INTO asset_controllo (asset_id, controllo_id, livello, assessor_soggetto_id, evidenza) VALUES
  (2, 2, 2, 2, 'Job backup attivo; restore test documentato solo su una finestra.'),
  (1, 3, 1, 2, 'Alert su host applicativo, ma log applicativi non centralizzati.'),
  (3, 3, 2, 2, 'Firewall log abilitati; correlazione eventi migliorabile.');
