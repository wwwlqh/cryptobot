"""Generate the Power BI project (PBIP) for the live-trade dashboard.

Usage:
    python analysis/powerbi/build_pbip.py

Writes analysis/powerbi/CryptobotTrades.pbip plus its .Report and .SemanticModel
folders. Open the .pbip in Power BI Desktop and press Refresh to load the data.
The CSV path is stored in the DataFile parameter (Transform data > Edit parameters).
"""
import json
import os
import shutil
import uuid
from pathlib import Path

HERE = Path(__file__).parent
NAME = "CryptobotTrades"
CSV = (HERE.parent / "data" / "trades.csv").resolve()
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric"

# Dark "trading desk" palette (validated: CVD-safe, >= 3:1 on the card surface).
BG = "#0F1420"
CARD = "#161C2B"
LINE = "#232B3E"
INK = "#E8EAF0"
INK_2 = "#9AA3B5"
SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#9085e9"]

# ---------------------------------------------------------------- semantic model

COLUMNS = [  # name, TMDL type, M type, format
    ("trade_id", "int64", "Int64.Type", "0"),
    ("bet_time_utc", "dateTime", "type datetime", "yyyy-mm-dd hh:nn"),
    ("bet_date", "dateTime", "type date", "yyyy-mm-dd"),
    ("bet_hour_utc", "int64", "Int64.Type", "0"),
    ("coin", "string", "type text", None),
    ("strategy", "string", "type text", None),
    ("window", "string", "type text", None),
    ("side", "string", "type text", None),
    ("model_prob", "double", "type number", "0.0%"),
    ("market_price", "double", "type number", "0.0%"),
    ("model_edge", "double", "type number", "0.0%"),
    ("fill_price", "double", "type number", "0.000"),
    ("stake_usd", "double", "type number", r"\$#,0.00"),
    ("is_top_up", "int64", "Int64.Type", "0"),
    ("outcome", "string", "type text", None),
    ("market_went_up", "int64", "Int64.Type", "0"),
    ("model_correct", "int64", "Int64.Type", "0"),
    ("pnl_usd", "double", "type number", r"\$#,0.00;-\$#,0.00"),
    ("seconds_into_window", "int64", "Int64.Type", "0"),
    ("cum_pnl_usd", "double", "type number", r"\$#,0.00;-\$#,0.00"),
    ("drawdown_usd", "double", "type number", r"\$#,0.00;-\$#,0.00"),
]

USD = r"\$#,0.00;-\$#,0.00;\$0.00"
PCT = "0.0%"
MEASURES = [  # name, DAX, format
    ("Trades", "COUNTROWS ( trades )", "#,0"),
    ("Resolved Trades", 'CALCULATE ( [Trades], trades[outcome] <> "Pending" )', "#,0"),
    ("Net PnL", "SUM ( trades[pnl_usd] )", USD),
    ("Total Staked", "SUM ( trades[stake_usd] )", r"\$#,0"),
    ("ROI %", "DIVIDE ( [Net PnL], [Total Staked] )", "0.00%"),
    ("Win Rate %", "DIVIDE ( CALCULATE ( [Trades], trades[pnl_usd] > 0 ), [Resolved Trades] )", PCT),
    ("Avg Win", "CALCULATE ( AVERAGE ( trades[pnl_usd] ), trades[pnl_usd] > 0 )", USD),
    ("Avg Loss", "CALCULATE ( AVERAGE ( trades[pnl_usd] ), trades[pnl_usd] < 0 )", USD),
    ("Model Accuracy %", "CALCULATE ( AVERAGE ( trades[model_correct] ), NOT ISBLANK ( trades[model_prob] ) )", PCT),
    ("Avg Model Prob %", "AVERAGE ( trades[model_prob] )", PCT),
    ("Avg Price Paid %", "CALCULATE ( AVERAGE ( trades[market_price] ), NOT ISBLANK ( trades[model_prob] ) )", PCT),
    ("Accuracy vs Price (pts)", "( [Model Accuracy %] - [Avg Price Paid %] ) * 100", "+0.0;-0.0"),
    ("Cumulative PnL",
     "VAR _upTo = MAX ( trades[bet_date] ) RETURN CALCULATE ( [Net PnL], trades[bet_date] <= _upTo, "
     "REMOVEFILTERS ( trades[bet_date], trades[bet_time_utc], trades[trade_id] ) )", USD),
    ("Max Drawdown", "MIN ( trades[drawdown_usd] )", USD),
]

PROB_BUCKET = (
    'SWITCH ( TRUE (), ISBLANK ( trades[model_prob] ), BLANK (), trades[model_prob] < 0.7, "60-70%", '
    'trades[model_prob] < 0.8, "70-80%", trades[model_prob] < 0.9, "80-90%", "90-100%" )'
)


def tmdl_table() -> str:
    out = ["table trades", ""]
    for name, dax, fmt in MEASURES:
        out += [f"\tmeasure '{name}' = {dax}", f"\t\tformatString: {fmt}", ""]
    for name, dtype, _, fmt in COLUMNS:
        out += [f"\tcolumn {name}", f"\t\tdataType: {dtype}"]
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        out += ["\t\tsummarizeBy: none", f"\t\tsourceColumn: {name}", ""]
    out += ["\tcolumn 'Prob Bucket' = " + PROB_BUCKET, "\t\tdataType: string", "\t\tsummarizeBy: none", ""]
    types = ", ".join(f'{{"{n}", {m}}}' for n, _, m, _ in COLUMNS)
    m = [
        "let",
        f'    Source = Csv.Document(File.Contents(DataFile), [Delimiter=",", Columns={len(COLUMNS)}, '
        "Encoding=65001, QuoteStyle=QuoteStyle.Csv]),",
        "    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),",
        f'    Typed = Table.TransformColumnTypes(Promoted, {{{types}}}, "en-US")',
        "in",
        "    Typed",
    ]
    out += ["\tpartition trades = m", "\t\tmode: import", "\t\tsource ="]
    out += ["\t\t\t\t" + line for line in m]
    return "\n".join(out) + "\n"


def write_model(root: Path) -> None:
    d = root / f"{NAME}.SemanticModel"
    (d / "definition" / "tables").mkdir(parents=True)
    dump(d / ".platform", platform("SemanticModel"))
    dump(d / "definition.pbism", {
        "$schema": f"{SCHEMA}/item/semanticModel/definitionProperties/1.0.0/schema.json",
        "version": "4.2", "settings": {}})
    (d / "definition" / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1600\n", encoding="utf-8")
    (d / "definition" / "model.tmdl").write_text(
        "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
        "\tsourceQueryCulture: en-US\n\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\n"
        "ref table trades\n", encoding="utf-8")
    (d / "definition" / "expressions.tmdl").write_text(
        f'expression DataFile = "{CSV}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]\n',
        encoding="utf-8")
    (d / "definition" / "tables" / "trades.tmdl").write_text(tmdl_table(), encoding="utf-8")


# ---------------------------------------------------------------- report helpers

def lit(v):
    return {"expr": {"Literal": {"Value": v}}}


def s(text):  # string literal
    return lit("'" + text.replace("'", "''") + "'")


def color(hex_):
    return {"solid": {"color": s(hex_)}}


def field(name, measure=True):
    kind = "Measure" if measure else "Column"
    return {kind: {"Expression": {"SourceRef": {"Entity": "trades"}}, "Property": name}}


def proj(name, measure=True, display=None):
    p = {"field": field(name, measure), "queryRef": f"trades.{name}", "nativeQueryRef": name}
    if display:
        p["displayName"] = display
    return p


class Page:
    def __init__(self, pid, title):
        self.pid, self.title, self.visuals = pid, title, []

    def add(self, vtype, x, y, w, h, query=None, objects=None, title=None, sort=None, extra=None):
        name = f"{self.pid}_{len(self.visuals):02d}"
        visual = {"visualType": vtype}
        if query:
            visual["query"] = {"queryState": {role: {"projections": ps} for role, ps in query.items()}}
            if sort:
                visual["query"]["sortDefinition"] = {"sort": [{"field": field(sort[0], sort[1]),
                                                              "direction": sort[2]}], "isDefaultSort": False}
        if objects:
            visual["objects"] = objects
        vco = {}
        if title:
            vco["title"] = [{"properties": {"show": lit("true"), "text": s(title)}}]
        if extra:
            vco.update(extra)
        if vco:
            visual["visualContainerObjects"] = vco
        self.visuals.append({
            "$schema": f"{SCHEMA}/item/report/definition/visualContainer/2.4.0/schema.json",
            "name": name,
            "position": {"x": x, "y": y, "z": len(self.visuals), "width": w, "height": h,
                         "tabOrder": len(self.visuals)},
            "visual": visual,
        })

    # -- building blocks --------------------------------------------------
    def text(self, x, y, w, h, runs):
        paragraphs = [{"textRuns": [{"value": t, "textStyle": st}]} for t, st in runs]
        self.add("textbox", x, y, w, h, objects={"general": [{"properties": {"paragraphs": paragraphs}}]},
                 extra={"background": [{"properties": {"show": lit("false")}}],
                        "border": [{"properties": {"show": lit("false")}}]})

    def header(self, title, subtitle):
        self.text(24, 8, 840, 76, [
            (title, {"fontWeight": "bold", "fontSize": "18pt", "color": INK}),
            (subtitle, {"fontSize": "10pt", "color": INK_2}),
        ])

    def card(self, x, y, w, h, measure, label):
        self.add("card", x, y, w, h, query={"Values": [proj(measure, display=label)]}, objects={
            "labels": [{"properties": {"color": color(INK), "fontSize": lit("24D")}}],
            "categoryLabels": [{"properties": {"show": lit("true"), "color": color(INK_2),
                                               "fontSize": lit("10D")}}],
        })

    def slicer(self, x, y, w, h, column, label):
        self.add("slicer", x, y, w, h, query={"Values": [proj(column, measure=False)]}, title=label, objects={
            "data": [{"properties": {"mode": s("Dropdown")}}],
            "header": [{"properties": {"show": lit("false")}}],
        })


def kpi_row(page, items, y=88, h=96):
    gap, left, total = 16, 24, 1232
    w = (total - gap * (len(items) - 1)) / len(items)
    for i, (measure, label) in enumerate(items):
        page.card(left + i * (w + gap), y, w, h, measure, label)


DATA_LABELS = {"labels": [{"properties": {"show": lit("true"), "color": color(INK), "fontSize": lit("9D")}}]}


# ---------------------------------------------------------------- pages

def overview():
    p = Page("overview", "Overview")
    p.header("Crypto Price Prediction Bot: live trading results",
             "5,384 live trades on Polymarket 5m / 15m up-down markets  ·  Jul 8 - Aug 21, 2026")
    p.slicer(900, 8, 170, 74, "coin", "Coin")
    p.slicer(1086, 8, 170, 74, "strategy", "Strategy")
    kpi_row(p, [("Trades", "Live trades"), ("Win Rate %", "Win rate"), ("Net PnL", "Net P&L"),
                ("ROI %", "Return on stake"), ("Max Drawdown", "Max drawdown")])
    p.add("lineChart", 24, 200, 800, 496,
          query={"Category": [proj("bet_date", measure=False, display="Date")],
                 "Y": [proj("Cumulative PnL", display="Cumulative P&L")]},
          title="Cumulative net P&L")
    p.add("clusteredBarChart", 840, 200, 416, 240,
          query={"Category": [proj("coin", measure=False, display="Coin")],
                 "Y": [proj("Net PnL", display="Net P&L")]},
          objects=DATA_LABELS, title="Net P&L by coin", sort=("Net PnL", True, "Descending"))
    p.add("clusteredColumnChart", 840, 456, 416, 240,
          query={"Category": [proj("outcome", measure=False, display="Outcome")],
                 "Y": [proj("Net PnL", display="Net P&L")]},
          objects=DATA_LABELS, title="Wins +$2,140 vs losses -$2,265", sort=("Net PnL", True, "Descending"))
    return p


def model_page():
    p = Page("model", "Model accuracy")
    p.header("Was the model right often enough?",
             "On Polymarket the price paid is the break-even hit rate. The model had to beat it, after fees.")
    kpi_row(p, [("Avg Model Prob %", "Model's average confidence"), ("Model Accuracy %", "Actual hit rate"),
                ("Avg Price Paid %", "Average price paid (break-even)"),
                ("Accuracy vs Price (pts)", "Hit rate minus price (pts)")])
    p.add("lineChart", 24, 200, 760, 496,
          query={"Category": [proj("Prob Bucket", measure=False, display="Model probability")],
                 "Y": [proj("Avg Model Prob %", display="Model said"),
                       proj("Avg Price Paid %", display="Price paid (break-even)"),
                       proj("Model Accuracy %", display="Actual hit rate")]},
          objects={"markers": [{"properties": {"show": lit("true")}}], **DATA_LABELS},
          title="Calibration: the model was overconfident at every confidence level",
          sort=("Prob Bucket", False, "Ascending"))
    p.add("pivotTable", 800, 200, 456, 236,
          query={"Rows": [proj("strategy", measure=False, display="Strategy"),
                          proj("window", measure=False, display="Window")],
                 "Values": [proj("Trades"), proj("Model Accuracy %", display="Hit rate"),
                            proj("Avg Price Paid %", display="Price paid"), proj("Net PnL", display="Net P&L")]},
          title="By strategy and window")
    p.text(800, 452, 456, 244, [
        ("What this shows", {"fontWeight": "bold", "fontSize": "13pt", "color": INK}),
        ("The model picked the right direction 73% of the time, but it bought at 75% on average. "
         "A 75-cent share only pays off if it wins more than 75% of the time, so a high win rate "
         "still lost money.", {"fontSize": "11pt", "color": INK_2}),
        ("When the model said 85%, it was right about 75% of the time.",
         {"fontSize": "11pt", "color": INK_2}),
    ])
    return p


def coins_page():
    p = Page("coins", "Coins & timing")
    p.header("Where the money was made and lost", "Per-coin breakdown, trade direction and time of day (UTC)")
    p.slicer(1086, 8, 170, 74, "window", "Window")
    p.add("tableEx", 24, 92, 1232, 336,
          query={"Values": [proj("coin", measure=False, display="Coin"), proj("Trades"),
                            proj("Win Rate %", display="Win rate"), proj("Model Accuracy %", display="Hit rate"),
                            proj("Avg Price Paid %", display="Price paid"), proj("Avg Win", display="Avg win"),
                            proj("Avg Loss", display="Avg loss"), proj("Net PnL", display="Net P&L"),
                            proj("ROI %", display="ROI")]},
          objects={"grid": [{"properties": {"textSize": lit("12D"), "rowPadding": lit("4D")}}],
                   "columnHeaders": [{"properties": {"fontSize": lit("11D")}}]},
          title="Per-coin scorecard", sort=("Net PnL", True, "Descending"))
    p.add("clusteredColumnChart", 24, 444, 808, 252,
          query={"Category": [proj("bet_hour_utc", measure=False, display="Hour (UTC)")],
                 "Y": [proj("Net PnL", display="Net P&L")]},
          title="Net P&L by hour of day (UTC)", sort=("bet_hour_utc", False, "Ascending"))
    p.add("clusteredBarChart", 848, 444, 408, 252,
          query={"Category": [proj("side", measure=False, display="Bet direction")],
                 "Y": [proj("Net PnL", display="Net P&L")]},
          objects=DATA_LABELS, title="Up bets vs down bets")
    return p


# ---------------------------------------------------------------- report

THEME = {
    "name": "Cryptobot Dark",
    "dataColors": SERIES,
    "background": CARD, "foreground": INK, "tableAccent": SERIES[0],
    "good": "#0ca30c", "neutral": "#c98500", "bad": "#d03b3b",
    "textClasses": {
        "callout": {"fontFace": "Segoe UI Semibold", "fontSize": 24, "color": INK},
        "title": {"fontFace": "Segoe UI Semibold", "fontSize": 12, "color": INK},
        "header": {"fontFace": "Segoe UI Semibold", "fontSize": 11, "color": INK},
        "label": {"fontFace": "Segoe UI", "fontSize": 10, "color": INK_2},
    },
    "visualStyles": {
        "*": {"*": {
            "background": [{"show": True, "color": {"solid": {"color": CARD}}, "transparency": 0}],
            "border": [{"show": True, "color": {"solid": {"color": LINE}}, "radius": 10}],
            "dropShadow": [{"show": False}],
            "title": [{"show": True, "fontColor": {"solid": {"color": INK}}, "fontSize": 12}],
            "visualHeader": [{"show": False}],
            "padding": [{"top": 10, "bottom": 10, "left": 12, "right": 12}],
            "categoryAxis": [{"labelColor": {"solid": {"color": INK_2}}, "titleColor": {"solid": {"color": INK_2}},
                              "gridlineShow": False}],
            "valueAxis": [{"labelColor": {"solid": {"color": INK_2}}, "titleColor": {"solid": {"color": INK_2}},
                           "gridlineColor": {"solid": {"color": LINE}}}],
            "legend": [{"labelColor": {"solid": {"color": INK_2}}, "position": "Top"}],
            "lineStyles": [{"strokeWidth": 3}],
        }},
        "page": {"*": {
            "background": [{"color": {"solid": {"color": BG}}, "transparency": 0}],
            "outspace": [{"color": {"solid": {"color": BG}}, "transparency": 0}],
        }},
        "tableEx": {"*": {"grid": [{"gridHorizontalColor": {"solid": {"color": LINE}},
                                    "rowPadding": 6}],
                          "columnHeaders": [{"fontColor": {"solid": {"color": INK_2}},
                                             "backColor": {"solid": {"color": CARD}}}],
                          "values": [{"fontColorPrimary": {"solid": {"color": INK}},
                                      "backColorPrimary": {"solid": {"color": CARD}},
                                      "fontColorSecondary": {"solid": {"color": INK}},
                                      "backColorSecondary": {"solid": {"color": "#1B2235"}}}]}},
        "pivotTable": {"*": {"columnHeaders": [{"fontColor": {"solid": {"color": INK_2}},
                                                "backColor": {"solid": {"color": CARD}}}],
                             "rowHeaders": [{"fontColor": {"solid": {"color": INK}},
                                             "backColor": {"solid": {"color": CARD}}}],
                             "values": [{"fontColorPrimary": {"solid": {"color": INK}},
                                         "backColorPrimary": {"solid": {"color": CARD}},
                                         "fontColorSecondary": {"solid": {"color": INK}},
                                         "backColorSecondary": {"solid": {"color": "#1B2235"}}}]}},
        "slicer": {"*": {"items": [{"fontColor": {"solid": {"color": INK}},
                                    "background": {"solid": {"color": CARD}}}]}},
    },
}


def write_report(root: Path, pages) -> None:
    d = root / f"{NAME}.Report"
    defn = d / "definition"
    (defn / "pages").mkdir(parents=True)
    res = d / "StaticResources" / "RegisteredResources"
    res.mkdir(parents=True)
    dump(d / ".platform", platform("Report"))
    dump(d / "definition.pbir", {
        "$schema": f"{SCHEMA}/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0", "datasetReference": {"byPath": {"path": f"../{NAME}.SemanticModel"}}})
    dump(defn / "version.json", {"$schema": f"{SCHEMA}/item/report/definition/versionMetadata/1.0.0/schema.json",
                                 "version": "2.0.0"})
    dump(res / "CryptobotDark.json", THEME)
    dump(defn / "report.json", {
        "$schema": f"{SCHEMA}/item/report/definition/report/3.0.0/schema.json",
        "themeCollection": {"customTheme": {
            "name": "CryptobotDark.json",
            "reportVersionAtImport": {"visual": "2.4.0", "page": "2.0.0", "report": "3.0.0"},
            "type": "RegisteredResources"}},
        "resourcePackages": [{"name": "RegisteredResources", "type": "RegisteredResources", "items": [
            {"name": "CryptobotDark.json", "path": "CryptobotDark.json", "type": "CustomTheme"}]}],
        "settings": {"useStylableVisualContainerHeader": True, "defaultDrillFilterOtherVisuals": True},
    })
    dump(defn / "pages" / "pages.json", {
        "$schema": f"{SCHEMA}/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [p.pid for p in pages],
        "activePageName": os.environ.get("PBI_START_PAGE", pages[0].pid)})
    for p in pages:
        pd_ = defn / "pages" / p.pid
        dump(pd_ / "page.json", {
            "$schema": f"{SCHEMA}/item/report/definition/page/2.0.0/schema.json",
            "name": p.pid, "displayName": p.title, "displayOption": "FitToPage",
            "height": 720, "width": 1280})
        for v in p.visuals:
            dump(pd_ / "visuals" / v["name"] / "visual.json", v)


def platform(kind):
    return {"$schema": f"{SCHEMA}/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": kind, "displayName": NAME},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid5(uuid.NAMESPACE_URL, NAME + kind))}}


def dump(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    for old in (HERE / f"{NAME}.Report", HERE / f"{NAME}.SemanticModel"):
        if old.exists():
            shutil.rmtree(old)
    write_model(HERE)
    write_report(HERE, [overview(), model_page(), coins_page()])
    dump(HERE / f"{NAME}.pbip", {
        "$schema": f"{SCHEMA}/pbip/pbipProperties/1.0.0/schema.json",
        "version": "1.0", "artifacts": [{"report": {"path": f"{NAME}.Report"}}],
        "settings": {"enableAutoRecovery": True}})
    print(f"wrote {HERE / (NAME + '.pbip')}  (data: {CSV})")
