import json
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, url_for

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "time-tracker-secret")

DATA_DIR = os.path.join(app.root_path, "data")
DATA_FILE = os.path.join(DATA_DIR, "time_tracker.xlsx")

LOG_COLUMNS = [
    "Date",
    "Project",
    "Activity",
    "Start",
    "End",
    "DurationMinutes",
    "Notes",
]
PROJECT_COLUMNS = ["Project", "Activity"]
ACTIVE_COLUMNS = ["Project", "Activity", "Notes", "Start"]


def ensure_workbook() -> None:
    """Create the Excel workbook with the expected sheets when missing."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(DATA_FILE):
        log_df = pd.DataFrame(columns=LOG_COLUMNS)
        projects_df = pd.DataFrame(columns=PROJECT_COLUMNS)
        active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
        with pd.ExcelWriter(DATA_FILE, engine="openpyxl") as writer:
            log_df.to_excel(writer, sheet_name="Log", index=False)
            projects_df.to_excel(writer, sheet_name="Projects", index=False)
            active_df.to_excel(writer, sheet_name="Active", index=False)


def read_workbook() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read the workbook sheets containing log, projects, and active entry."""
    ensure_workbook()
    with pd.ExcelFile(DATA_FILE) as xls:
        log_df = pd.read_excel(xls, "Log")
        projects_df = pd.read_excel(xls, "Projects")
        try:
            active_df = pd.read_excel(xls, "Active")
        except ValueError:
            active_df = pd.DataFrame(columns=ACTIVE_COLUMNS)
    return log_df, projects_df, active_df


def write_workbook(
    log_df: pd.DataFrame,
    projects_df: pd.DataFrame,
    active_df: Optional[pd.DataFrame] = None,
) -> None:
    """Persist the workbook sheets with the supplied dataframes."""
    os.makedirs(DATA_DIR, exist_ok=True)
    log_to_write = log_df.copy()
    if not log_to_write.empty:
        for column in ("Date", "Start", "End"):
            if column in log_to_write.columns:
                log_to_write[column] = pd.to_datetime(
                    log_to_write[column], errors="coerce"
                )
        if "Date" in log_to_write.columns:
            log_to_write["Date"] = log_to_write["Date"].dt.date
    projects_to_write = projects_df.copy()
    active_to_write = (
        active_df.copy()
        if active_df is not None
        else pd.DataFrame(columns=ACTIVE_COLUMNS)
    )
    if not active_to_write.empty and "Start" in active_to_write.columns:
        active_to_write["Start"] = pd.to_datetime(
            active_to_write["Start"], errors="coerce"
        )
    with pd.ExcelWriter(DATA_FILE, engine="openpyxl") as writer:
        log_to_write.to_excel(writer, sheet_name="Log", index=False)
        projects_to_write.to_excel(writer, sheet_name="Projects", index=False)
        active_to_write.to_excel(writer, sheet_name="Active", index=False)


def normalize_log_dataframe(log_df: pd.DataFrame) -> pd.DataFrame:
    if log_df.empty:
        return pd.DataFrame(columns=LOG_COLUMNS)
    normalized = log_df.copy()
    if "Date" in normalized.columns:
        normalized["Date"] = pd.to_datetime(normalized["Date"], errors="coerce").dt.date
    for column in ("Start", "End"):
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    if "DurationMinutes" in normalized.columns:
        normalized["DurationMinutes"] = pd.to_numeric(
            normalized["DurationMinutes"], errors="coerce"
        ).fillna(0)
    else:
        normalized["DurationMinutes"] = 0
    return normalized


def build_project_map(projects_df: pd.DataFrame) -> Dict[str, List[str]]:
    if projects_df.empty:
        return {}
    cleaned = projects_df.fillna("")
    cleaned["Project"] = cleaned["Project"].astype(str).str.strip()
    cleaned["Activity"] = cleaned["Activity"].astype(str).str.strip()
    grouped: Dict[str, List[str]] = {}
    for project, group in cleaned.groupby("Project"):
        activities = sorted(
            {activity for activity in group["Activity"].tolist() if activity}
        )
        if project:
            grouped[project] = activities
    return grouped


def _safe_string(value: object) -> str:
    if isinstance(value, str):
        return value
    if pd.isna(value):
        return ""
    return str(value)


def add_project_activity(project: str, activity: str) -> Tuple[bool, str]:
    project_name = project.strip()
    activity_name = activity.strip()
    if not project_name or not activity_name:
        return False, "Project and activity names are both required."
    log_df, projects_df, active_df = read_workbook()
    if not projects_df.empty:
        existing = projects_df[
            projects_df["Project"].astype(str).str.lower() == project_name.lower()
        ]
        if not existing.empty:
            match = existing[
                existing["Activity"].astype(str).str.lower() == activity_name.lower()
            ]
            if not match.empty:
                return False, "This activity is already defined for the project."
    new_row = pd.DataFrame([{ "Project": project_name, "Activity": activity_name }])
    updated_projects = pd.concat([projects_df, new_row], ignore_index=True)
    write_workbook(log_df, updated_projects, active_df)
    return True, f"Added '{activity_name}' to {project_name}."


def format_duration(minutes: float) -> str:
    total_seconds = int(round(minutes * 60))
    hours, remainder = divmod(total_seconds, 3600)
    mins, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:02d}h {mins:02d}m"
    if mins:
        return f"{mins}m {secs:02d}s"
    return f"{secs}s"


def format_week_label(week_start: datetime) -> str:
    if isinstance(week_start, datetime):
        start_dt = week_start
    else:
        start_dt = datetime.combine(week_start, datetime.min.time())
    week_end = start_dt + timedelta(days=6)
    return f"{start_dt.strftime('%b %d, %Y')} – {week_end.strftime('%b %d, %Y')}"


def build_summary(log_df: pd.DataFrame) -> Dict[str, List[Tuple[str, str]]]:
    if log_df.empty:
        return {
            "daily": [],
            "weekly": [],
            "monthly": [],
            "yearly": [],
            "activities": [],
            "projects": [],
        }
    df = log_df.copy()
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df = df.dropna(subset=["Start"])
    df["DurationMinutes"] = pd.to_numeric(df["DurationMinutes"], errors="coerce").fillna(0)

    df["DateOnly"] = df["Start"].dt.date
    daily_series = df.groupby("DateOnly")["DurationMinutes"].sum().sort_index(ascending=False)
    daily = [(date.strftime("%b %d, %Y"), format_duration(minutes)) for date, minutes in daily_series.items()]

    df["WeekStart"] = df["Start"].dt.to_period("W").apply(lambda p: p.start_time.date())
    weekly_series = df.groupby("WeekStart")["DurationMinutes"].sum().sort_index(ascending=False)
    weekly = [
        (format_week_label(pd.Timestamp(week).to_pydatetime()), format_duration(minutes))
        for week, minutes in weekly_series.items()
    ]

    df["MonthPeriod"] = df["Start"].dt.to_period("M")
    monthly_series = df.groupby("MonthPeriod")["DurationMinutes"].sum().sort_index(ascending=False)
    monthly = [
        (period.strftime("%b %Y"), format_duration(minutes))
        for period, minutes in monthly_series.items()
    ]

    df["YearValue"] = df["Start"].dt.year
    yearly_series = df.groupby("YearValue")["DurationMinutes"].sum().sort_index(ascending=False)
    yearly = [(str(year), format_duration(minutes)) for year, minutes in yearly_series.items()]

    activity_df = df.copy()
    activity_df["Project"] = activity_df["Project"].astype(str).str.strip()
    activity_df["Activity"] = activity_df["Activity"].astype(str).str.strip()
    activity_series = (
        activity_df[(activity_df["Project"] != "") & (activity_df["Activity"] != "")]
        .groupby(["Project", "Activity"])["DurationMinutes"]
        .sum()
        .sort_values(ascending=False)
    )
    activities = [
        (f"{project} · {activity}", format_duration(minutes))
        for (project, activity), minutes in activity_series.items()
    ]

    project_series = (
        activity_df[activity_df["Project"] != ""]
        .groupby("Project")["DurationMinutes"]
        .sum()
        .sort_values(ascending=False)
    )
    projects = [(project, format_duration(minutes)) for project, minutes in project_series.items()]

    return {
        "daily": daily,
        "weekly": weekly,
        "monthly": monthly,
        "yearly": yearly,
        "activities": activities,
        "projects": projects,
    }


def get_recent_logs(log_df: pd.DataFrame, limit: int = 10) -> List[Dict[str, str]]:
    if log_df.empty:
        return []
    df = log_df.copy()
    df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
    df["End"] = pd.to_datetime(df["End"], errors="coerce")
    df = df.dropna(subset=["Start", "End"])
    recent = df.sort_values("Start", ascending=False).head(limit)
    entries: List[Dict[str, str]] = []
    for _, row in recent.iterrows():
        date_value = row.get("Date", "")
        if isinstance(date_value, pd.Timestamp):
            date_value = date_value.date()
        if isinstance(date_value, datetime):
            date_display = date_value.strftime("%b %d, %Y")
        elif isinstance(date_value, date):
            date_display = date_value.strftime("%b %d, %Y")
        else:
            date_display = str(date_value) if date_value else ""
        entries.append(
            {
                "date": date_display,
                "project": row.get("Project", ""),
                "activity": row.get("Activity", ""),
                "start": row["Start"].strftime("%b %d, %Y %I:%M %p"),
                "end": row["End"].strftime("%b %d, %Y %I:%M %p"),
                "duration": format_duration(row.get("DurationMinutes", 0)),
                "notes": row.get("Notes", ""),
            }
        )
    return entries


def build_active_entry(active_df: pd.DataFrame) -> Dict[str, str]:
    if active_df.empty:
        return {}
    row = active_df.iloc[0].to_dict()
    start_value = row.get("Start")
    start_dt = pd.to_datetime(start_value, errors="coerce")
    if pd.isna(start_dt):
        return {}
    return {
        "project": _safe_string(row.get("Project", "")),
        "activity": _safe_string(row.get("Activity", "")),
        "notes": _safe_string(row.get("Notes", "")),
        "start_display": start_dt.strftime("%b %d, %Y %I:%M %p"),
        "start_iso": start_dt.isoformat(),
    }


@app.route("/", methods=["GET", "POST"])
def index():
    ensure_workbook()
    if request.method == "POST":
        action = request.form.get("action")
        if action == "start":
            return handle_start()
        if action == "stop":
            return handle_stop()
        flash("Unsupported action.", "error")
        return redirect(url_for("index"))

    log_df, projects_df, active_df = read_workbook()
    normalized_log = normalize_log_dataframe(log_df)
    project_map = build_project_map(projects_df)
    summary = build_summary(normalized_log)
    recent_logs = get_recent_logs(normalized_log)
    active_entry = build_active_entry(active_df)

    return render_template(
        "index.html",
        projects=project_map,
        project_names=sorted(project_map.keys()),
        activities_json=json.dumps(project_map),
        summary=summary,
        recent_logs=recent_logs,
        active_entry=active_entry,
    )


def handle_start():
    log_df, projects_df, active_df = read_workbook()
    if not active_df.empty:
        flash("A timer is already running. Stop it before starting a new one.", "error")
        return redirect(url_for("index"))
    project = request.form.get("project", "").strip()
    activity = request.form.get("activity", "").strip()
    notes = request.form.get("notes", "").strip()
    if not project:
        flash("Please choose a project before starting the timer.", "error")
        return redirect(url_for("index"))
    if not activity:
        flash("Please choose an activity before starting the timer.", "error")
        return redirect(url_for("index"))
    project_map = build_project_map(projects_df)
    if project not in project_map or activity not in project_map[project]:
        flash("The selected project and activity are not defined.", "error")
        return redirect(url_for("index"))
    start_time = datetime.now()
    active_entry_df = pd.DataFrame(
        [
            {
                "Project": project,
                "Activity": activity,
                "Notes": notes,
                "Start": start_time,
            }
        ]
    )
    write_workbook(log_df, projects_df, active_entry_df)
    flash(f"Timer started for {activity} in {project}.", "success")
    return redirect(url_for("index"))


def handle_stop():
    log_df, projects_df, active_df = read_workbook()
    if active_df.empty:
        flash("No running timer to stop.", "error")
        return redirect(url_for("index"))
    active_entry = active_df.iloc[0].to_dict()
    notes = request.form.get("notes", "").strip()
    start_dt = pd.to_datetime(active_entry.get("Start"), errors="coerce")
    if pd.isna(start_dt):
        flash("The active timer has an invalid start time and was cleared.", "error")
        write_workbook(log_df, projects_df, pd.DataFrame(columns=ACTIVE_COLUMNS))
        return redirect(url_for("index"))
    end_dt = datetime.now()
    duration_minutes = round((end_dt - start_dt).total_seconds() / 60, 2)
    entry = {
        "Date": start_dt.date(),
        "Project": active_entry.get("Project", ""),
        "Activity": active_entry.get("Activity", ""),
        "Start": start_dt,
        "End": end_dt,
        "DurationMinutes": duration_minutes,
        "Notes": notes or active_entry.get("Notes", ""),
    }
    updated_log = pd.concat([log_df, pd.DataFrame([entry])], ignore_index=True)
    write_workbook(updated_log, projects_df, pd.DataFrame(columns=ACTIVE_COLUMNS))
    flash("Session saved to the time log.", "success")
    return redirect(url_for("index"))


@app.route("/add-project", methods=["POST"])
def add_project():
    project = request.form.get("new_project", "")
    activity = request.form.get("new_activity", "")
    success, message = add_project_activity(project, activity)
    flash(message, "success" if success else "error")
    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True)
