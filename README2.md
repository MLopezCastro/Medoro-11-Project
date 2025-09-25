
# MEDORO 11 – Pipeline con Vistas (versión “Vista Única Completa”)

**Objetivo:** publicar una única vista analítica con **3 años móviles**, **bloques por día**, **secuencia global 1..N** y **atributos enriquecidos** (joins).
Power BI consume **solo** `dbo.ConCuboSecuenciasBloques_Rango_M11_Completa`.

## Qué resuelve

* **Volumen controlado:** la ventana móvil de **3 años** evita mover millones de filas históricas.
* **Lectura más simple:** se colapsan eventos a **bloques por OT + Renglón + Día**.
* **Atributos extra:** se agrega `saccod1` (UNION) + `OP, CodAlfa, CodMaq, Alto, Ancho, AltoV, Fuelle` (NEW).
* **Secuencia Global:** un índice 1..N estable para ordenar historias de producción.

---

## Diagrama (resumen)

```
ConCubo  ──► V1 ConCubo3Años ─► V2 ConCubo3AñosSec ─► V3 ConCubo3AñosSecFlag
                                     │
TablaVinculadaUNION ──► (saccod1) ───┼──► V4 ConCuboSecuenciasBloques_M11_Completa
TablaVinculadaNEW   ──► (OP, Alto…) ──┘
                                          └─► V5 ConCuboSecuenciasBloques_Rango_M11_Completa  ⇐ (vista FINAL para PBI)
```

---

## Requisitos

* `dbo.ConCubo` (eventos crudos)
* `dbo.TablaVinculadaUNION` (tiene `OP` y `saccod1`)
* `dbo.TablaVinculadaNEW` (tiene `OP`, `NroGlobal`, `CodAlfa`, `CodMaq`, `Alto`, `Ancho`, `AltoV`, `Fuelle`)

> ⚠️ Si alguna es **vista**, no pasa nada; los índices recomendados solo aplican si son **tablas**.

---

## Scripts (copiar/pegar y ejecutar en este orden)

### V1 – Últimos 3 años + correcciones

```sql
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
```

### V2 – Duración real entre fechas corregidas

```sql
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
```

### V3 – Flags y secuencias de preparación

```sql
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
```

### V4 – **Completa** (colapso a BLOQUES + joins + atributos + primer Turno/Maquinista/Operario/Motivo del bloque)

```sql
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_M11_Completa AS
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
Base AS ( -- traigo métricas + atributos que quiero exponer en bloque
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
    FROM dbo.ConCubo3AñosSecFlag s
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
ConRN AS ( -- nro en el día para poder “primer valor del bloque”
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
```

### V5 – **Vista FINAL** para Power BI (Orden + Secuencia Global)

```sql
CREATE OR ALTER VIEW dbo.ConCuboSecuenciasBloques_Rango_M11_Completa AS
SELECT
    d.*,
    -- orden estable (yyyyMMddHHmmss + Renglón + ID)
    REPLACE(REPLACE(REPLACE(CONVERT(varchar(19), d.InicioSecuencia, 120),'-',''),' ',''),':','')
    + RIGHT('0000' + CAST(d.Renglon AS varchar(4)), 4)
    + RIGHT('0000000000' + CAST(d.ID_Limpio AS varchar(10)), 10) AS OrdenGlobalText,
    -- índice global 1..N
    ROW_NUMBER() OVER (ORDER BY d.InicioSecuencia, d.Renglon, d.ID_Limpio) AS SecuenciaGlobalSQL
FROM dbo.ConCuboSecuenciasBloques_M11_Completa AS d;
GO
```

---

## Índices recomendados (opcionales, una sola vez)

> Aplican **solo si el objeto es tabla (`'U'`)**. Si es vista, saltealo.

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

-- UNION: por OP (si es tabla)
--IF OBJECT_ID('dbo.TablaVinculadaUNION','U') IS NOT NULL
--BEGIN
--  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_UNION_OP' AND object_id=OBJECT_ID('dbo.TablaVinculadaUNION'))
--    CREATE INDEX IX_UNION_OP ON dbo.TablaVinculadaUNION (OP);
--END
--GO

-- ConCubo: ayuda a ventanas (si es tabla)
--IF OBJECT_ID('dbo.ConCubo','U') IS NOT NULL
--BEGIN
--  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_ConCubo_Inicio_Renglon_ID' AND object_id=OBJECT_ID('dbo.ConCubo'))
--    CREATE INDEX IX_ConCubo_Inicio_Renglon_ID ON dbo.ConCubo (Inicio, Renglon, ID);
--END
--GO
```

**Cómo usarlos**

* Crealos **una sola vez**.
* No tocan la lógica; solo mejoran los JOIN y los ORDER BY en las vistas.

---

## Cómo consumir en Power BI

1. Conectá al SQL y seleccioná **solo** `dbo.ConCuboSecuenciasBloques_Rango_M11_Completa`.
2. En visuals secuenciales, **ordená por** `SecuenciaGlobalSQL` o `OrdenGlobalText`.
3. Usá slicers de **la misma vista** (fechas por `FechaSecuencia` y filtros por `Renglon`, `ID_Limpio`, etc.).
4. Si querés cargar menos columnas, desmarcá lo que no uses en el navegador de campos (ayuda al preview).

---

## Checks rápidos

```sql
-- ¿Salió algo?
SELECT TOP 20 ID_Limpio, CONVERT(varchar(16), InicioSecuencia,120) AS Inicio,
       SecuenciaGlobalSQL, Renglon, OP, CodMaq, saccod1
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completa
ORDER BY SecuenciaGlobalSQL;

-- Comparativa ejemplo (Renglon 201 – año 2024)
SELECT COUNT(*) Bloques, SUM(BuenosTotal) Buenos, SUM(HorasProd) Prod
FROM dbo.ConCuboSecuenciasBloques_Rango_M11_Completa
WHERE Renglon=201 AND FechaSecuencia>='2024-01-01' AND FechaSecuencia<'2025-01-01';
```

---

## Notas y alternativas

* Este README usa **solo vistas** (nada materializado).
* Si PBI sigue lento, alternativa: materializar en tablas “fact” + **columnstore** y refresco incremental con stored procedures.
* Para José, esta versión evita cambios de infra: **1 vista final** con todo (tiempos, cantidades, atributos, secuencia global).

---


