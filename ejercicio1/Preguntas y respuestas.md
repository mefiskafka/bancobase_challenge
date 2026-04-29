# Ejercicio 1 — Preguntas y respuestas

> Todo esta Documentado en [README.md](README.md) e implementado en [Airflow/dags/etl_engineer_challenge.py](Airflow/dags/etl_engineer_challenge.py). El perfilado se ejecutó sobre las 10 000 filas de `data_prueba_tecnica.csv`.

## 1. Para los ids nulos ¿Qué sugieres hacer con ellos?

**Para los datos nulos se creo un bucket de cuarentena**. La regla aplicada en el pipeline es:

- Si `id` es `null` o cadena vacía → la fila se aísla en `s3://bck-bronze/quarantine/data_prueba_tecnica_quarantine.parquet` con la columna booleana `flag_id_missing=true`.
- La fila **no** entra a `bronze.prueba.tbl_data`, por lo que no contamina los KPIs.
- Queda **disponible** para su posterior revision.




## 2. Considerando las columnas `name` y `company_id` ¿Qué inconsistencias notas y cómo las mitigas?

### `name y company_id`

Se encontraron datos que estan mal formateados y que no se pueden inferir, la decision fue enviarlos al bucket de cuarentena para su posterior revision con los stakeholders.



## 3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo procedes?

Sí. Al perfilar el CSV se observa mucha data basura sembrada en varias columnas. Por ejemplo: Se detectaron Outliers, se encontraron float64 desbordados tambien.
En status, created_at y paid_at de igual forma se almacenan en el bucket cuarentena.

Ademas el archivo viene con `\r\r\n` (CRLF doble), tambien se encontraron id duplicados


## 4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones?


1. **Falta una arquitectura medallon (Bronze > Silver > Gold).** Hoy comprimimos limpieza + servicio en bronze. Una capa silver con dimensiones conformadas (`dim_merchant`, `dim_date`) hubiera sido lo mejor.
2. En luhgar de migrar a parquet podemos migrar a Iceberg o Delta Lake. para asegurar ACID y time travel.
3. Importante tener Particionado `tbl_data` por `year`/`month` de `created_at`.
4. Las reglas de calidad de datos viven en código Python, seria mejor hacer esa transformacion con dbt para tener la trazabilidad del dato.


## 5. Captura de pantalla de la query con Trino (DBeaver)

Las capturas con las queries ejecutadas en Trino quedaron en el [README.md](README.md) — sección **Cómo levantar el entorno** (capturas `Trino`, `trino_data`, `comprobacion_datos`, `Trino_querys`).


Una mejora futura que me gusta tambien es enviar las filas en cuarentena a un mecanismo automatizado (Slack / correo / Jira) que dispare un ticket al dueño del sistema fuente con la fila ofectada.