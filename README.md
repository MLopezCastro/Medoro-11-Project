
# MEDORO 11 — README

## Qué es

Medoro 11 es la evolución del pipeline de M10. Mantiene la lógica base (V1–V3) y agrega:

* **Dos joins nuevos** en la vista de bloques:

  * `TablaVinculadaUNION` → `saccod1` (por OP numérica).
  * `TablaVinculadaNEW` → `OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle` (por `NroGlobal`/`OP`).
* **Vista final única para Power BI**: `dbo.ConCuboSecuenciasBloques_Rango_M11`, que ya incluye **SecuenciaGlobalSQL** y todas las columnas nuevas.
* Optimizaciones (sin `FORMAT`, parsing robusto de fechas/OP, joins deduplicados).

## Qué cambia vs M10

* Se usan **las mismas 3 vistas iniciales** (V1–V3): `ConCubo3Años`, `ConCubo3AñosSec`, `ConCubo3AñosSecFlag`.
* La vista de **bloques (V4)** ahora trae también `saccod1` + columnas físicas de `…NEW`.
* La vista **final (V5)** agrega `OrdenGlobalText` y **`SecuenciaGlobalSQL`** (1..N global) y es la **única** que usás en PBI.

## Prerrequisitos

En la base destino (ej. `Sispro_16092025`) deben existir:

* `dbo.ConCubo` (eventos crudos).
* `dbo.TablaVinculadaUNION` (con `OP` y `saccod1`).
* `dbo.TablaVinculadaNEW` (con `OP, NroGlobal, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle`).

---

# Script ÚNICO (crear todo)

> Pegar completo y ejecutar en SSMS. Crea/actualiza V1..V5 y, **opcional**, los índices sugeridos para acelerar los joins.

```sql
/* =============================================================
   MEDORO 11 – Pipeline simple SOLO CON VISTAS (recomendado)
   Base destino: Sispro_16092025
   Resultado final PBI: dbo.ConCuboSecuenciasBloques_Rango_M11
   ============================================================= */

USE Sispro_16092025;
GO

/* ─────────────────────────────────────────────────────────────
   PRE-REQUISITOS (deben existir):
   - dbo.ConCubo
   - dbo.TablaVinculadaUNION   (OP, saccod1)
   - dbo.TablaVinculadaNEW     (OP, NroGlobal, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle)
   ───────────────────────────────────────────────────────────── */

/* =============================================================
   V1 – ConCubo3Años (como M10)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3Años AS
WITH DatosParseados AS (
    SELECT * ,
           TRY_CAST(Inicio AS DATETIME) AS InicioDT,
           TRY_CAST(Fin    AS DATETIME) AS FinDT
    FROM dbo.ConCubo
    WHERE TRY_CAST(Inicio AS DATETIME) >= DATEADD(YEAR, -3, CAST(GETDATE() AS DATE))
      AND ISNUMERIC(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID))) = 1
),
HorasCalculadas AS (
    SELECT * ,
           DATEDIFF(SECOND, InicioDT, FinDT) / 3600.0 AS Total_Horas
    FROM DatosParseados
)
SELECT
    ID,
    TRY_CAST(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID)) AS INT) AS ID_Limpio,
    Renglon, Estado,
    DATEADD(DAY, -2, InicioDT) AS Inicio_Corregido,
    DATEADD(DAY, -2, FinDT)    AS Fin_Corregido,
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, InicioDT), 120) AS Inicio_Legible_Texto,
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, FinDT)   , 120) AS Fin_Legible_Texto,
    CONVERT(DATE, DATEADD(DAY, -2, InicioDT)) AS Fecha,
    Total_Horas,
    CASE WHEN Estado='Producción'     THEN Total_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado='Preparación'    THEN Total_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado='Maquina Parada' THEN Total_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado='Mantenimiento'  THEN Total_Horas ELSE 0 END AS Horas_Mantenimiento,
    TRY_CAST(CantidadBuenosProducida AS FLOAT) AS CantidadBuenosProducida,
    TRY_CAST(CantidadMalosProducida  AS FLOAT) AS CantidadMalosProducida,
    Turno, Maquinista, Operario, codproducto, motivo
FROM HorasCalculadas;
GO

/* =============================================================
   V2 – ConCubo3AñosSec (como M10)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSec AS
WITH Base AS (
    SELECT * ,
           DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 AS Duracion_Horas
    FROM dbo.ConCubo3Años
)
SELECT
    ID, ID_Limpio, Renglon, Estado,
    Inicio_Corregido, Fin_Corregido,
    Inicio_Legible_Texto, Fin_Legible_Texto,
    CONVERT(DATE, Inicio_Corregido) AS Fecha,
    Duracion_Horas AS Total_Horas,
    CASE WHEN Estado='Producción'     THEN Duracion_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado='Preparación'    THEN Duracion_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado='Maquina Parada' THEN Duracion_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado='Mantenimiento'  THEN Duracion_Horas ELSE 0 END AS Horas_Mantenimiento,
    CantidadBuenosProducida, CantidadMalosProducida,
    Turno, Maquinista, Operario, codproducto, Motivo
FROM Base;
GO

/* =============================================================
   V3 – ConCubo3AñosSecFlag (como M10)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSecFlag AS
WITH Base AS (
    SELECT * ,
           ROW_NUMBER() OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido) AS Nro_Secuencia
    FROM dbo.ConCubo3AñosSec
),
PrepFlag AS (
    SELECT * ,
           CASE WHEN Estado='Preparación'
                 AND LAG(Estado) OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido)
                     IS DISTINCT FROM 'Preparación'
                THEN 1 ELSE 0 END AS FlagPreparacion
    FROM Base
),
PrepSecuencia AS (
    SELECT * ,
           SUM(FlagPreparacion) OVER (
              PARTITION BY ID_Limpio, Renglon
              ORDER BY Inicio_Corregido
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS SecuenciaPreparacion
    FROM PrepFlag
)
SELECT
  ID, ID_Limpio, Renglon, Estado,
  Inicio_Corregido, Fin_Corregido,
  Inicio_Legible_Texto, Fin_Legible_Texto,
  Fecha, Total_Horas,
  Horas_Produccion, Horas_Preparacion, Horas_Parada, Horas_Mantenimiento,
  CantidadBuenosProducida, CantidadMalosProducida,
  Turno, Maquinista, Operario, CodProducto, Motivo,
  Nro_Secuencia, FlagPreparacion, SecuenciaPreparacion
FROM PrepSecuencia;
GO

/* =============================================================
   V4 – ConCuboSecuenciasBloques_M11 (joins y colapsado)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_M11 AS
WITH VU AS (
    SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
    FROM dbo.TablaVinculadaUNION
    WHERE ISNUMERIC(OP)=1
    GROUP BY TRY_CAST(OP AS INT)
),
NEW_base AS (
    SELECT
        COALESCE(
            TRY_CAST(NroGlobal AS INT),
            TRY_CAST(SUBSTRING(OP, PATINDEX('%[0-9]%', OP), 50) AS INT)
        ) AS ID_Limpio,
        OP, CodAlfa, CodMaq,
        TRY_CAST(Alto   AS INT) AS Alto,
        TRY_CAST(Ancho  AS INT) AS Ancho,
        TRY_CAST(AltoV  AS INT) AS AltoV,
        TRY_CAST(Fuelle AS INT) AS Fuelle
    FROM dbo.TablaVinculadaNEW
),
NEWmap AS (
    SELECT
        ID_Limpio,
        MAX(OP)      AS OP,
        MAX(CodAlfa) AS CodAlfa,
        MAX(CodMaq)  AS CodMaq,
        MAX(Alto)    AS Alto,
        MAX(Ancho)   AS Ancho,
        MAX(AltoV)   AS AltoV,
        MAX(Fuelle)  AS Fuelle
    FROM NEW_base
    WHERE ID_Limpio IS NOT NULL
    GROUP BY ID_Limpio
),
Base AS (
    SELECT
        s.Renglon, s.ID, s.ID_Limpio,
        s.Inicio_Corregido, s.Fin_Corregido,
        CAST(ISNULL(s.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS CantBuenos,
        CAST(ISNULL(s.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS CantMalos,
        CAST(ISNULL(s.Horas_Produccion       ,0) AS DECIMAL(18,6)) AS HorasProd,
        CAST(ISNULL(s.Horas_Preparacion      ,0) AS DECIMAL(18,6)) AS HorasPrep,
        CAST(ISNULL(s.Horas_Parada           ,0) AS DECIMAL(18,6)) AS HorasPara,
        CAST(ISNULL(s.Horas_Mantenimiento    ,0) AS DECIMAL(18,6)) AS HorasMant,
        s.CodProducto
    FROM dbo.ConCubo3AñosSecFlag s
    WHERE s.Inicio_Corregido IS NOT NULL AND s.Fin_Corregido IS NOT NULL
),
Marcado AS (
    SELECT * ,
           CASE WHEN LAG(ID_Limpio) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido)=ID_Limpio
                THEN 0 ELSE 1 END AS CambioID
    FROM Base
),
Grupos AS (
    SELECT * ,
           SUM(CambioID) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS GrupoOT
    FROM Marcado
),
Dia AS (
    SELECT
        Renglon, ID, ID_Limpio, GrupoOT,
        CONVERT(date, Inicio_Corregido) AS FechaSecuencia,
        MIN(Inicio_Corregido) AS InicioSecuencia,
        MAX(Fin_Corregido)    AS FinSecuencia,
        MAX(CodProducto)      AS CodProducto_Bloque,
        SUM(CantBuenos)       AS BuenosTotal,
        SUM(CantMalos)        AS MalosTotal,
        SUM(HorasProd)        AS HorasProd,
        SUM(HorasPrep)        AS HorasPrep,
        SUM(HorasPara)        AS HorasPara,
        SUM(HorasMant)        AS HorasMant,
        COUNT(*)              AS FilasColapsadas
    FROM Grupos
    GROUP BY Renglon, GrupoOT, ID, ID_Limpio, CONVERT(date, Inicio_Corregido)
)
SELECT
    d.Renglon, d.ID, d.ID_Limpio,
    d.CodProducto_Bloque AS CodProducto,
    d.FechaSecuencia,
    CONVERT(varchar(16), d.InicioSecuencia, 120) AS FechaSecuenciaTextoHora,
    d.InicioSecuencia, d.FinSecuencia,
    d.BuenosTotal, d.MalosTotal, d.HorasProd, d.HorasPrep, d.HorasPara, d.HorasMant,
    d.FilasColapsadas,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia
        ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio
    ) AS NumeroBloqueDiaSQL,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia, d.Renglon
        ORDER BY d.InicioSecuencia, d.ID_Limpio
    ) AS NumeroBloqueDiaPorRenglonSQL,

    CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','') AS bigint) * 10000000000
      + CAST(d.Renglon AS bigint) * 1000000000
      + CAST(d.ID_Limpio AS bigint)                 AS SortKey,

    VU.saccod1,
    N.OP, N.CodAlfa, N.CodMaq,
    N.Alto, N.Ancho, N.AltoV, N.Fuelle
FROM Dia d
LEFT JOIN VU     ON VU.ID_Limpio  = d.ID_Limpio
LEFT JOIN NEWmap N ON N.ID_Limpio = d.ID_Limpio;
GO

/* =============================================================
   V5 – ConCuboSecuenciasBloques_Rango_M11 (vista FINAL para PBI)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11 AS
SELECT
    d.*,
    -- OrdenGlobalText: yyyyMMddHHmmss + Renglon (4) + ID (10)
    REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','')
    + RIGHT('0000' + CAST(d.Renglon AS varchar(4)), 4)
    + RIGHT('0000000000' + CAST(d.ID_Limpio AS varchar(10)), 10) AS OrdenGlobalText,

    -- Índice global 1..N
    ROW_NUMBER() OVER (ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio) AS SecuenciaGlobalSQL
FROM dbo.ConCuboSecuenciasBloques_M11 AS d;
GO

/* =============================================================
   ÍNDICES OPCIONALES (una sola vez) – aceleran joins/lecturas
   ============================================================= */
IF OBJECT_ID('dbo.TablaVinculadaNEW','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_NEW_NroGlobal' AND object_id=OBJECT_ID('dbo.TablaVinculadaNEW'))
    CREATE INDEX IX_NEW_NroGlobal
      ON dbo.TablaVinculadaNEW (NroGlobal)
      INCLUDE (OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle);

  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_NEW_OP' AND object_id=OBJECT_ID('dbo.TablaVinculadaNEW'))
    CREATE INDEX IX_NEW_OP
      ON dbo.TablaVinculadaNEW (OP)
      INCLUDE (NroGlobal, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle);
END
GO

-- (Opcional) si UNION es TABLA:
--IF OBJECT_ID('dbo.TablaVinculadaUNION','U') IS NOT NULL
--BEGIN
--  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UNION_OP' AND object_id=OBJECT_ID('dbo.TablaVinculadaUNION'))
--    CREATE INDEX IX_UNION_OP ON dbo.TablaVinculadaUNION (OP);
--END
--GO

-- (Opcional) si ConCubo es TABLA:
--IF OBJECT_ID('dbo.ConCubo','U') IS NOT NULL
--BEGIN
--  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ConCubo_Inicio_Renglon_ID' AND object_id=OBJECT_ID('dbo.ConCubo'))
--    CREATE INDEX IX_ConCubo_Inicio_Renglon_ID ON dbo.ConCubo (Inicio, Renglon, ID);
--END
--GO

/* =============================================================
   CHECKS RÁPIDOS
   ============================================================= */
-- ¿Hay filas en la final?
-- SELECT TOP 20 ID_Limpio, FechaSecuencia, SecuenciaGlobalSQL,
--               OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle, saccod1
-- FROM dbo.ConCuboSecuenciasBloques_Rango_M11
-- ORDER BY SecuenciaGlobalSQL;

-- Ejemplo de validación (Renglon 201, año 2024):
-- SELECT COUNT(*) Bloques,
--        SUM(BuenosTotal) Buenos,
--        SUM(HorasProd)   Prod,
--        SUM(HorasPrep)   Prep,
--        SUM(HorasPara)   Para,
--        SUM(HorasMant)   Mant
-- FROM dbo.ConCuboSecuenciasBloques_Rango_M11
-- WHERE Renglon=201
--   AND FechaSecuencia >= '2024-01-01'
--   AND FechaSecuencia <  '2025-01-01';
```

---

## Cómo usarlo en Power BI

1. Conectá a SQL Server → **Seleccioná SOLO** `dbo.ConCuboSecuenciasBloques_Rango_M11`.
2. En Power Query, dejá únicamente las columnas que necesitás (Choose Columns).
3. Ordená los visuals que lo requieran por **`SecuenciaGlobalSQL`**.
4. Filtrá por **FechaSecuencia** cuando hagas cortes (mantiene folding).

## Tips de performance

* Evitamos `FORMAT()` → menos CPU.
* Deduplicamos `TablaVinculadaNEW` por `ID_Limpio` antes del join (evita multiplicar filas).
* Índices `IX_NEW_NroGlobal` y `IX_NEW_OP` ayudan si NEW es **tabla**.

## (Opcional) Compatibilidad con nombres de M10

Si José quiere seguir usando los nombres viejos sin tocar su PBIX:

```sql
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques       AS SELECT * FROM dbo.ConCuboSecuenciasBloques_M11;
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango AS SELECT * FROM dbo.ConCuboSecuenciasBloques_Rango_M11;
```

Listo. Con esto tenés Medoro 11 **documentado, reproducible y con una sola vista final** para PBI. Si mañana querés sumar **Refresh incremental de PBI**, lo armamos arriba de esta vista final sin tocar SQL.
