# Genera grafana/dashboards/tablero_vuelos.json (no hace falta ejecutarlo: el JSON ya viene en la carpeta).
import json
from pathlib import Path

DS = {"type": "grafana-postgresql-datasource", "uid": "aws-vuelos"}
FILTRO = "anio IN ($anio) AND nombre_aerolinea IN ($aerolinea)"
TASA = "SUM(retrasados) * 100.0 / NULLIF(SUM(operados), 0)"
UMBRALES = {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                          {"color": "#EAB839", "value": 15},
                                          {"color": "red", "value": 20}]}

def consulta(sql, formato="table"):
    return [{"refId": "A", "datasource": DS, "editorMode": "code", "rawQuery": True,
             "format": formato, "rawSql": sql.strip()}]

def panel(pid, tipo, titulo, x, y, w, h, sql, descripcion="", formato="table", defaults=None,
          overrides=None, options=None):
    return {"id": pid, "type": tipo, "title": titulo, "description": descripcion, "datasource": DS,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": consulta(sql, formato),
            "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []},
            "options": options or {}}

def stat(pid, titulo, x, sql, unidad="short", decimales=0, umbrales=None, descripcion=""):
    return panel(pid, "stat", titulo, x, 0, 6, 4, sql, descripcion,
                 defaults={"unit": unidad, "decimals": decimales,
                           "color": {"mode": "thresholds" if umbrales else "fixed", "fixedColor": "#5794F2"},
                           "thresholds": umbrales or {"mode": "absolute", "steps": [{"color": "#5794F2", "value": None}]}},
                 options={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                          "colorMode": "background" if umbrales else "value", "graphMode": "none",
                          "textMode": "value", "justifyMode": "center", "orientation": "auto"})

paneles = [
    stat(1, "Vuelos analizados", 0, f"SELECT SUM(vuelos) AS vuelos FROM analitica_vuelos WHERE {FILTRO}", "locale",
         descripcion="Vuelos programados en los años y aerolíneas elegidos (después de la limpieza)."),
    stat(2, "% de vuelos con retraso", 6, f"SELECT {TASA} AS tasa FROM analitica_vuelos WHERE {FILTRO}",
         "percent", 2, UMBRALES,
         "Vuelos operados que llegaron 15 minutos o más tarde. Verde < 15 %, amarillo 15–20 %, rojo ≥ 20 % "
         "(los mismos límites de niveles_puntualidad.csv)."),
    stat(3, "% de vuelos cancelados", 12,
         f"SELECT SUM(cancelados) * 100.0 / NULLIF(SUM(vuelos), 0) AS tasa FROM analitica_vuelos WHERE {FILTRO}",
         "percent", 2),
    stat(4, "Retraso medio de llegada", 18,
         f"SELECT SUM(suma_retraso_min) / NULLIF(SUM(operados), 0) AS minutos FROM analitica_vuelos WHERE {FILTRO}",
         "m", 1, descripcion="Minutos promedio de diferencia entre la llegada real y la programada "
                             "(los vuelos que llegan antes restan)."),

    panel(5, "timeseries", "% de vuelos con retraso por mes", 0, 4, 16, 8,
          f"""SELECT periodo AS time, {TASA} AS "% retraso"
FROM analitica_vuelos WHERE {FILTRO}
GROUP BY periodo ORDER BY periodo""",
          "Evolución mensual. Las líneas punteadas marcan los límites de 15 % y 20 %.", formato="time_series",
          defaults={"unit": "percent", "decimals": 1, "min": 0, "color": {"mode": "fixed", "fixedColor": "#5794F2"},
                    "thresholds": UMBRALES,
                    "custom": {"lineWidth": 2, "fillOpacity": 10, "pointSize": 5, "showPoints": "always",
                               "thresholdsStyle": {"mode": "dashed"}}},
          options={"legend": {"showLegend": False}, "tooltip": {"mode": "single"}}),

    panel(6, "piechart", "Minutos de retraso por causa", 16, 4, 8, 8,
          f"""SELECT SUM(min_aerolinea) AS "Aerolínea",
       SUM(min_avion_retrasado) AS "Avión que llega tarde",
       SUM(min_sistema_aereo) AS "Sistema aéreo nacional",
       SUM(min_clima) AS "Clima extremo",
       SUM(min_seguridad) AS "Seguridad"
FROM analitica_vuelos WHERE {FILTRO}""",
          "Reparto de los minutos de retraso según la causa que reporta la aerolínea.",
          defaults={"unit": "short", "decimals": 1},
          options={"pieType": "donut", "displayLabels": ["percent"],
                   "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                   "legend": {"showLegend": True, "displayMode": "table", "placement": "bottom",
                              "values": ["percent"]}}),

    panel(7, "barchart", "% de retraso por aerolínea", 0, 12, 12, 10,
          f"""SELECT nombre_aerolinea AS "Aerolínea", {TASA} AS "% retraso"
FROM analitica_vuelos WHERE {FILTRO}
GROUP BY nombre_aerolinea ORDER BY 2 DESC""",
          "Ordenadas de mayor a menor retraso. El color sigue los niveles de puntualidad.",
          defaults={"unit": "percent", "decimals": 1, "color": {"mode": "thresholds"}, "thresholds": UMBRALES,
                    "custom": {"fillOpacity": 85, "lineWidth": 0}},
          options={"orientation": "horizontal", "xField": "Aerolínea", "showValue": "always",
                   "barWidth": 0.8, "legend": {"showLegend": False}, "tooltip": {"mode": "single"}}),

    panel(8, "barchart", "% de retraso por hora programada de salida", 12, 12, 12, 10,
          """SELECT LPAD(hora_salida::text, 2, '0') || ':00' AS "Hora",
       SUM(retrasados) * 100.0 / NULLIF(SUM(operados), 0) AS "% retraso"
FROM analitica_horas WHERE anio IN ($anio)
GROUP BY hora_salida ORDER BY hora_salida""",
          "Todas las aerolíneas (la tabla analitica_horas no se separa por aerolínea); solo responde al filtro de año.",
          defaults={"unit": "percent", "decimals": 0, "color": {"mode": "thresholds"}, "thresholds": UMBRALES,
                    "custom": {"fillOpacity": 85, "lineWidth": 0}},
          options={"orientation": "vertical", "xField": "Hora", "showValue": "never", "barWidth": 0.85,
                   "xTickLabelRotation": -45, "legend": {"showLegend": False}, "tooltip": {"mode": "single"}}),

    panel(9, "barchart", "% de retraso por mes del año", 0, 22, 9, 15,
          f"""SELECT (ARRAY['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'])[mes] AS "Mes",
       {TASA} AS "% retraso"
FROM analitica_vuelos WHERE {FILTRO}
GROUP BY mes ORDER BY mes""",
          "Temporada: todos los años elegidos juntos, mes por mes.",
          defaults={"unit": "percent", "decimals": 1, "color": {"mode": "thresholds"}, "thresholds": UMBRALES,
                    "custom": {"fillOpacity": 85, "lineWidth": 0}},
          options={"orientation": "vertical", "xField": "Mes", "showValue": "auto", "barWidth": 0.8,
                   "legend": {"showLegend": False}, "tooltip": {"mode": "single"}}),

    panel(10, "table", "Los 10 aeropuertos de origen con más vuelos", 9, 22, 15, 15,
          f"""SELECT origin AS "Aeropuerto",
       SUM(vuelos) AS "Vuelos",
       {TASA} AS "% retraso",
       SUM(cancelados) * 100.0 / NULLIF(SUM(vuelos), 0) AS "% cancelados",
       SUM(suma_retraso_min) / NULLIF(SUM(operados), 0) AS "Retraso medio (min)",
       CASE WHEN {TASA} < 15 THEN 'Alta' WHEN {TASA} < 20 THEN 'Media' ELSE 'Baja' END AS "Puntualidad"
FROM analitica_vuelos WHERE {FILTRO}
GROUP BY origin ORDER BY 2 DESC LIMIT 10""",
          "Nivel de puntualidad con los límites de niveles_puntualidad.csv: Alta < 15 %, Media 15–20 %, Baja ≥ 20 %.",
          defaults={"decimals": 1, "custom": {"align": "auto", "cellOptions": {"type": "auto"}}},
          overrides=[
              {"matcher": {"id": "byName", "options": "Vuelos"},
               "properties": [{"id": "unit", "value": "locale"}, {"id": "decimals", "value": 0}]},
              {"matcher": {"id": "byName", "options": "% retraso"},
               "properties": [{"id": "unit", "value": "percent"}, {"id": "thresholds", "value": UMBRALES},
                              {"id": "color", "value": {"mode": "thresholds"}},
                              {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}}]},
              {"matcher": {"id": "byName", "options": "% cancelados"},
               "properties": [{"id": "unit", "value": "percent"}, {"id": "decimals", "value": 2}]},
              {"matcher": {"id": "byName", "options": "Puntualidad"},
               "properties": [{"id": "mappings", "value": [{"type": "value", "options": {
                   "Alta": {"color": "green", "index": 0}, "Media": {"color": "#EAB839", "index": 1},
                   "Baja": {"color": "red", "index": 2}}}]},
                   {"id": "custom.cellOptions", "value": {"type": "color-text"}}]},
          ],
          options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}),

    panel(11, "table", "Ejecuciones del flujo de Prefect (tabla registro_cargas)", 0, 37, 24, 7,
          """SELECT fecha_ejecucion AS "Fecha de ejecución",
       archivos_nuevos AS "Archivos nuevos",
       anios_en_analitica AS "Años en la analítica",
       vuelos_analizados AS "Vuelos analizados",
       duracion_s AS "Duración (s)"
FROM registro_cargas ORDER BY fecha_ejecucion DESC""",
          "Una fila por cada vez que corre el flujo: muestra cuándo se actualizaron los datos del tablero.",
          defaults={"custom": {"align": "auto"}},
          overrides=[{"matcher": {"id": "byName", "options": "Fecha de ejecución"},
                      "properties": [{"id": "unit", "value": "dateTimeAsLocal"}]},
                     {"matcher": {"id": "byName", "options": "Vuelos analizados"},
                      "properties": [{"id": "unit", "value": "locale"}]}],
          options={"showHeader": True, "cellHeight": "sm", "footer": {"show": False}}),
]

def variable(nombre, etiqueta, sql):
    return {"type": "query", "name": nombre, "label": etiqueta, "datasource": DS,
            "query": sql, "definition": sql, "refresh": 1, "multi": True, "includeAll": True,
            "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "sort": 1,
            "hide": 0, "options": [], "skipUrlSync": False}

tablero = {
    "uid": "retrasos-vuelos",
    "title": "Retrasos de vuelos en EE. UU. – Tablero gerencial",
    "description": "Resultados de la analítica guardados en AWS (base vuelos_analitica) por el flujo de Prefect.",
    "tags": ["vuelos", "aws", "big data"],
    "timezone": "utc",
    "editable": True,
    "graphTooltip": 1,
    "time": {"from": "2016-01-01T00:00:00.000Z", "to": "2018-12-31T23:59:59.000Z"},
    "timepicker": {},
    "refresh": "",
    "schemaVersion": 39,
    "version": 2,
    "templating": {"list": [
        variable("anio", "Año", "SELECT DISTINCT anio FROM analitica_vuelos ORDER BY anio"),
        variable("aerolinea", "Aerolínea", "SELECT DISTINCT nombre_aerolinea FROM analitica_vuelos ORDER BY nombre_aerolinea"),
    ]},
    "annotations": {"list": []},
    "panels": paneles,
}

salida = Path(__file__).parent / "dashboards" / "tablero_vuelos.json"
salida.parent.mkdir(exist_ok=True)
salida.write_text(json.dumps(tablero, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"Tablero guardado en {salida}")
