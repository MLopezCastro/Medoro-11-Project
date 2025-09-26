# Medoro 11 — README v2 (Approach **_Completo**)

Este documento deja **todo en un solo lugar**: explicación + **scripts SQL completos** (V1…V5) + **índices opcionales** + **consultas de validación**.
El objetivo es que puedas crear/actualizar las vistas y conectar **una sola vista final** a Power BI.

---

## 🎯 Objetivo

* Trabajar con una **ventana móvil de 3 años** para mantener el volumen manejable.
* **Colapsar eventos** en **bloques diarios** (por Línea/Renglón + Orden/ID).
* Exponer **atributos operativos** (Turno, Maquinista, Operario, Motivo).
* Enriquecer con **atributos de producto** (`OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle`) y `saccod1`.
* Proveer una **Secuencia Global 1..N** estable (`SecuenciaGlobalSQL`) para ordenar toda la historia de forma consistente.

---

## 🔗 Orígenes requeridos

Deben existir (como **tablas o vistas**):

* `dbo.ConCubo` (eventos crudos)
* `dbo.TablaVinculadaUNION` (OP, `saccod1`)
* `dbo.TablaVinculadaNEW` (OP, `NroGlobal`, `CodAlfa`, `CodMaq`, `Alto`, `Ancho`, `AltoV`, `Fuelle`)

> Si algún origen es **vista**, los índices sugeridos **no aplican** (solo funcionan en **tablas**).

---

## 🧩 Vistas creadas (todas terminan en `_Completo`)

1. `ConCubo3Años_Completo`

   * Filtra últimos **3 años** desde hoy.
   * Corrige fechas **-2 días** (desfase histórico).
   * Normaliza `ID_Limpio`, separa horas por estado.
   * Alias estandarizados: `CodProducto`, `Motivo`.

2. `ConCubo3AñosSec_Completo`

   * Duración real entre `Inicio_Corregido` y `Fin_Corregido`.
   * Mantiene cantidades y campos operativos.

3. `ConCubo3AñosSecFlag_Completo`

   * `Nro_Secuencia` por OT + Renglón.
   * `FlagPreparacion` y `SecuenciaPreparacion`.

4. `ConCuboSecuenciasBloques_M11_Completo`

   * Colapsa a **bloques diarios** por (Renglón + OT).
   * Toma **Turno/Maquinista/Operario/Motivo** del **primer evento** del bloque.
   * Joins:

     * `TablaVinculadaUNION` → `saccod1` (por `OP` numérico).
     * `TablaVinculadaNEW` (deduplicada) → `OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle`.
   * `SortKey` sin `FORMAT` (usa `DECIMAL(38,0)` para evitar overflow).

5. `ConCuboSecuenciasBloques_Rango_M11_Completo` **(vista final para Power BI)**

   * Agrega:

     * `OrdenGlobalText` (`yyyyMMddHHmmss` + línea(4) + OT(10))
     * `SecuenciaGlobalSQL` = `ROW_NUMBER()` global 1..N (no se reinicia).

---

## 🧱 Scripts SQL (copiar/pegar tal cual)

> ⚠️ Ajustá el `USE` si tu base tiene otro nombre.

```sql
/* =============================================================
   MEDORO 11 – Vistas COMPLETO (V1..V5)
   ============================================================= */
USE Sispro_16092025;
GO

/* =============================================================
   V1 – ConCubo3Años_Completo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3Años_Completo AS
WITH DatosParseados AS (
    SELECT *,
           TRY_CAST(Inicio AS DATETIME) AS InicioDT,
           TRY_CAST(Fin    AS DATETIME) AS FinDT
    FROM dbo.ConCubo
    WHERE TRY_CAST(Inicio AS DATETIME) >= DATEADD(YEAR, -3, CAST(GETDATE() AS DATE))
      AND ISNUMERIC(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID))) = 1
),
HorasCalculadas AS (
    SELECT *,
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
    Turno, Maquinista, Operario,
    codproducto AS CodProducto,
    motivo      AS Motivo
FROM HorasCalculadas;
GO

/* =============================================================
   V2 – ConCubo3AñosSec_Completo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSec_Completo AS
WITH Base AS (
    SELECT *,
           DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 AS Duracion_Horas
    FROM dbo.ConCubo3Años_Completo
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
    Turno, Maquinista, Operario, CodProducto, Motivo
FROM Base;
GO

/* =============================================================
   V3 – ConCubo3AñosSecFlag_Completo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCubo3AñosSecFlag_Completo AS
WITH Base AS (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY ID_Limpio, Renglon
             ORDER BY Inicio_Corregido
           ) AS Nro_Secuencia
    FROM dbo.ConCubo3AñosSec_Completo
),
PrepFlag AS (
    SELECT *,
           CASE WHEN Estado='Preparación'
                 AND LAG(Estado) OVER (PARTITION BY ID_Limpio, Renglon ORDER BY Inicio_Corregido)
                     IS DISTINCT FROM 'Preparación'
                THEN 1 ELSE 0 END AS FlagPreparacion
    FROM Base
),
PrepSecuencia AS (
    SELECT *,
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
   V4 – ConCuboSecuenciasBloques_M11_Completo
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_M11_Completo AS
WITH VU AS (  -- saccod1 por OP numérico
    SELECT TRY_CAST(OP AS INT) AS ID_Limpio, MIN(saccod1) AS saccod1
    FROM dbo.TablaVinculadaUNION
    WHERE ISNUMERIC(OP)=1
    GROUP BY TRY_CAST(OP AS INT)
),
NEW_base AS ( -- normalizo NEW y calculo ID_Limpio
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
NEWmap AS ( -- deduplico NEW: una fila por ID
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
Base AS ( -- métricas + atributos base
    SELECT
        s.Renglon, s.ID, s.ID_Limpio,
        s.Inicio_Corregido, s.Fin_Corregido,
        CAST(ISNULL(s.CantidadBuenosProducida,0) AS DECIMAL(18,4)) AS CantBuenos,
        CAST(ISNULL(s.CantidadMalosProducida ,0) AS DECIMAL(18,4)) AS CantMalos,
        CAST(ISNULL(s.Horas_Produccion       ,0) AS DECIMAL(18,6)) AS HorasProd,
        CAST(ISNULL(s.Horas_Preparacion      ,0) AS DECIMAL(18,6)) AS HorasPrep,
        CAST(ISNULL(s.Horas_Parada           ,0) AS DECIMAL(18,6)) AS HorasPara,
        CAST(ISNULL(s.Horas_Mantenimiento    ,0) AS DECIMAL(18,6)) AS HorasMant,
        s.CodProducto,
        s.Turno, s.Maquinista, s.Operario, s.Motivo
    FROM dbo.ConCubo3AñosSecFlag_Completo s
    WHERE s.Inicio_Corregido IS NOT NULL AND s.Fin_Corregido IS NOT NULL
),
Marcado AS (
    SELECT *,
           CASE WHEN LAG(ID_Limpio) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido)=ID_Limpio
                THEN 0 ELSE 1 END AS CambioID
    FROM Base
),
Grupos AS (
    SELECT *,
           SUM(CambioID) OVER (PARTITION BY Renglon ORDER BY Inicio_Corregido
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS GrupoOT
    FROM Marcado
),
ConRN AS ( -- nro en el día para “primer valor del bloque”
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY Renglon, GrupoOT, CONVERT(date, Inicio_Corregido)
             ORDER BY Inicio_Corregido
           ) AS rnBloque
    FROM Grupos
),
Dia AS ( -- colapso diario OT+renglón
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
        COUNT(*)              AS FilasColapsadas,
        -- atributos del primer evento del bloque
        MAX(CASE WHEN rnBloque=1 THEN Turno      END) AS Turno,
        MAX(CASE WHEN rnBloque=1 THEN Maquinista END) AS Maquinista,
        MAX(CASE WHEN rnBloque=1 THEN Operario   END) AS Operario,
        MAX(CASE WHEN rnBloque=1 THEN Motivo     END) AS Motivo
    FROM ConRN
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
    d.Turno, d.Maquinista, d.Operario, d.Motivo,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia
        ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio
    ) AS NumeroBloqueDiaSQL,

    ROW_NUMBER() OVER (
        PARTITION BY d.FechaSecuencia, d.Renglon
        ORDER BY d.InicioSecuencia, d.ID_Limpio
    ) AS NumeroBloqueDiaPorRenglonSQL,

    -- SortKey sin FORMAT (DECIMAL para evitar overflow)
    CAST(REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','') AS DECIMAL(38,0)) * 10000000000
      + CAST(d.Renglon   AS DECIMAL(38,0)) * 1000000000
      + CAST(d.ID_Limpio AS DECIMAL(38,0))                                    AS SortKey,

    VU.saccod1,
    N.OP, N.CodAlfa, N.CodMaq,
    N.Alto, N.Ancho, N.AltoV, N.Fuelle
FROM Dia d
LEFT JOIN VU     ON VU.ID_Limpio  = d.ID_Limpio
LEFT JOIN NEWmap N ON N.ID_Limpio = d.ID_Limpio;
GO

/* =============================================================
   V5 – ConCuboSecuenciasBloques_Rango_M11_Completo  (FINAL PBI)
   ============================================================= */
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11_Completo AS
SELECT
    d.*,

    -- orden textual yyyyMMddHHmmss + Renglon(4) + OT(10)
    REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','')
    + RIGHT('0000' + CAST(d.Renglon AS varchar(4)), 4)
    + RIGHT('0000000000' + CAST(d.ID_Limpio AS varchar(10)), 10) AS OrdenGlobalText,

    -- índice global 1..N (no se reinicia)
    ROW_NUMBER() OVER (ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio) AS SecuenciaGlobalSQL
FROM dbo.ConCuboSecuenciasBloques_M11_Completo AS d;
GO
```

---

## ⚡ Índices opcionales (crear **una** vez; solo si los objetos son **tablas**)

> Si son **vistas**, estos índices **no aplican**.

```sql
-- NEW: claves de join + columnas usadas
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

-- UNION: por OP
IF OBJECT_ID('dbo.TablaVinculadaUNION','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UNION_OP' AND object_id=OBJECT_ID('dbo.TablaVinculadaUNION'))
    CREATE INDEX IX_UNION_OP ON dbo.TablaVinculadaUNION (OP);
END
GO

-- ConCubo: ayuda a ventanas
IF OBJECT_ID('dbo.ConCubo','U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ConCubo_Inicio_Renglon_ID' AND object_id=OBJECT_ID('dbo.ConCubo'))
    CREATE INDEX IX_ConCubo_Inicio_Renglon_ID ON dbo.ConCubo (Inicio, Renglon, ID);
END
GO
```

---

## ✅ Checks rápidos

```sql
-- Muestra de la vista final
SELECT TOP 20 ID_Limpio, Renglon,
       CONVERT(varchar(16), InicioSecuencia,120) AS Inicio,
       SecuenciaGlobalSQL,
       Turno, Maquinista, Operario, Motivo,
       OP, CodMaq, saccod1,
       Alto, Ancho, AltoV, Fuelle
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo
ORDER BY SecuenciaGlobalSQL;

-- Totales ejemplo (Renglón 201, año 2024)
SELECT COUNT(*) Bloques, SUM(BuenosTotal) Buenos, SUM(HorasProd) Prod
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completo
WHERE Renglon=201
  AND FechaSecuencia >= '2024-01-01'
  AND FechaSecuencia <  '2025-01-01';
```

---

## 🖥️ Power BI — Cómo consumir

* Conectá **solo** a `dbo.ConCuboSecuenciasBloques_Rango_M11_Completo`.
* Para navegación temporal estable, **ordená** por `SecuenciaGlobalSQL`.
* Si el **preview** pesa, **desmarcá** columnas que no uses en el conector.

---

## 🧠 Tips de performance

* Evitá `FORMAT()` en SQL; usá `CONVERT/REPLACE` (más rápido).
* Deduplicá `TablaVinculadaNEW` **antes** del join (como hace `NEWmap`).
* No mezcles **granos** distintos en el mismo visual (evento vs bloque).
* Si hiciera falta más velocidad: materializar como tablas *fact* + columnstore, o **incremental refresh** en PBI (otro flujo).

---


