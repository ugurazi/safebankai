-- =========================
-- TABLES
-- =========================
CREATE TABLE IF NOT EXISTS MUST_TUM (
  MUST_NO INT NOT NULL,
  MUST_TC VARCHAR(11),
  ISKOLU_KOD VARCHAR(5),
  SUBE_KOD VARCHAR(10),
  IL_KOD VARCHAR(5),
  TELNO VARCHAR(15),        -- ✅ YENİ
  snapshot_date DATE,
  PRIMARY KEY (MUST_NO, snapshot_date)
);

CREATE TABLE IF NOT EXISTS SUBE_DIM (
  SUBE_KOD VARCHAR(10) PRIMARY KEY,
  SUBE_AD VARCHAR(255),
  SUBE_GRUP_KOD VARCHAR(10),
  IL_KOD VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS IL_DIM (
  IL_KOD VARCHAR(5) PRIMARY KEY,
  IL_AD VARCHAR(100)
);

-- =========================
-- CLEAN (DEMO)
-- =========================
DELETE FROM MUST_TUM;
DELETE FROM SUBE_DIM;
DELETE FROM IL_DIM;

-- =========================
-- IL DIM
-- =========================
INSERT INTO IL_DIM (IL_KOD, IL_AD) VALUES
('06','Ankara'),
('34','İstanbul');

-- =========================
-- SUBE DIM
-- =========================
INSERT INTO SUBE_DIM (SUBE_KOD, SUBE_AD, SUBE_GRUP_KOD, IL_KOD) VALUES
('2222','Çankaya Şube','G1','06'),
('3333','Kadıköy Şube','G2','34');

-- =========================
-- MUST_TUM (snapshot: 2025-12-31)
-- X = özel bankacılık
-- =========================
INSERT INTO MUST_TUM 
(MUST_NO, MUST_TC, ISKOLU_KOD, SUBE_KOD, IL_KOD, TELNO, snapshot_date) 
VALUES
(1001,'11111111111','X','2222','06','05321234567','2025-12-31'),
(1002,'22222222222','X','2222','06',NULL,'2025-12-31'),
(1003,'33333333333','X','3333','34','05439876543','2025-12-31'),
(1004,'44444444444','Y','3333','34',NULL,'2025-12-31'); -- Y: özel değil
