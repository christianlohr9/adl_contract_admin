import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  useCalendar,
  useCreateCalendar,
  useUpdateCalendar,
} from "@/api/queries/calendar";
import type { SeasonCalendarSchema } from "@/api/types";

const DEFAULT_SEASON = 2026;

// Readable labels for each date field
const FIELD_LABELS: Record<string, string> = {
  oext_deadline: "Offseason Extension Deadline",
  iext_window_start: "In-Season Extension Window Start",
  iext_window_end: "In-Season Extension Window End",
  tag_deadline: "Franchise Tag Deadline",
  tender_deadline: "RFA/ERFA Tender Deadline",
  br_deadline: "Buyout & Restructure Deadline",
  fyo_deadline: "5th Year Option Deadline",
  rookie_signing_deadline: "Rookie Signing Deadline",
  salary_rankings_date: "Salary Rankings Date",
  rf_auction_start: "R/F Auction Start",
  rf_auction_end: "R/F Auction End",
  ft_auction_start: "FT Auction Start",
  ft_auction_end: "FT Auction End",
  rfa_auction_start: "RFA Auction Start",
  rfa_auction_end: "RFA Auction End",
  br_auction_start: "B/R Auction Start",
  br_auction_end: "B/R Auction End",
  rookie_draft_start: "Rookie Draft Start",
  rookie_draft_end: "Rookie Draft End",
  udfa_auction_start: "UDFA Auction Start",
  udfa_auction_end: "UDFA Auction End",
  ufa_auction_start: "UFA Auction Start",
  regular_season_start: "Regular Season Start",
  regular_season_end: "Regular Season End",
};

// All editable date field keys (excludes id, season, created_at, updated_at)
type DateFieldKey = keyof Omit<
  SeasonCalendarSchema,
  "id" | "season" | "created_at" | "updated_at"
>;

const ALL_DATE_FIELDS: DateFieldKey[] = [
  "oext_deadline",
  "iext_window_start",
  "iext_window_end",
  "tag_deadline",
  "tender_deadline",
  "br_deadline",
  "fyo_deadline",
  "rookie_signing_deadline",
  "salary_rankings_date",
  "rf_auction_start",
  "rf_auction_end",
  "ft_auction_start",
  "ft_auction_end",
  "rfa_auction_start",
  "rfa_auction_end",
  "br_auction_start",
  "br_auction_end",
  "rookie_draft_start",
  "rookie_draft_end",
  "udfa_auction_start",
  "udfa_auction_end",
  "ufa_auction_start",
  "regular_season_start",
  "regular_season_end",
];

type FormData = Record<DateFieldKey, string>;

function buildEmptyForm(): FormData {
  const form = {} as FormData;
  for (const key of ALL_DATE_FIELDS) {
    form[key] = "";
  }
  return form;
}

function populateForm(calendar: SeasonCalendarSchema): FormData {
  const form = {} as FormData;
  for (const key of ALL_DATE_FIELDS) {
    form[key] = calendar[key] ?? "";
  }
  return form;
}

function buildPayload(season: number, form: FormData): Record<string, unknown> {
  const payload: Record<string, unknown> = { season };
  for (const key of ALL_DATE_FIELDS) {
    payload[key] = form[key] || null;
  }
  return payload;
}

const dateInputClass =
  "h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50";

interface DateFieldProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
}

function DateField({ label, value, onChange }: DateFieldProps) {
  return (
    <div className="space-y-1.5">
      <label className="text-sm font-medium leading-none">{label}</label>
      <input
        type="date"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={dateInputClass}
      />
    </div>
  );
}

interface DateRangeFieldProps {
  label: string;
  startValue: string;
  endValue: string;
  onStartChange: (value: string) => void;
  onEndChange: (value: string) => void;
}

function DateRangeField({
  label,
  startValue,
  endValue,
  onStartChange,
  onEndChange,
}: DateRangeFieldProps) {
  return (
    <div className="space-y-1.5">
      <label className="text-sm font-medium leading-none">{label}</label>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <span className="text-xs text-muted-foreground">Start</span>
          <input
            type="date"
            value={startValue}
            onChange={(e) => onStartChange(e.target.value)}
            className={dateInputClass}
          />
        </div>
        <div>
          <span className="text-xs text-muted-foreground">End</span>
          <input
            type="date"
            value={endValue}
            onChange={(e) => onEndChange(e.target.value)}
            className={dateInputClass}
          />
        </div>
      </div>
    </div>
  );
}

export function CalendarPage() {
  const [season, setSeason] = useState(DEFAULT_SEASON);
  const [inputSeason, setInputSeason] = useState(String(DEFAULT_SEASON));
  const [form, setForm] = useState<FormData>(buildEmptyForm);
  const [isNewSeason, setIsNewSeason] = useState(false);
  const [successMessage, setSuccessMessage] = useState("");

  const {
    data: calendar,
    isLoading,
    isError,
  } = useCalendar(season);

  const createMutation = useCreateCalendar();
  const updateMutation = useUpdateCalendar(season);

  const calendarExists = !!calendar && !isError;

  // Populate form when calendar data loads
  useEffect(() => {
    if (calendar && !isNewSeason) {
      setForm(populateForm(calendar));
    }
  }, [calendar, isNewSeason]);

  function handleLoad() {
    const parsed = parseInt(inputSeason, 10);
    if (!isNaN(parsed) && parsed > 2000 && parsed < 2100) {
      setSeason(parsed);
      setIsNewSeason(false);
      setSuccessMessage("");
    }
  }

  function handleNewSeason() {
    const parsed = parseInt(inputSeason, 10);
    if (!isNaN(parsed) && parsed > 2000 && parsed < 2100) {
      setSeason(parsed);
      setForm(buildEmptyForm());
      setIsNewSeason(true);
      setSuccessMessage("");
    }
  }

  function updateField(key: DateFieldKey, value: string) {
    setForm((prev) => ({ ...prev, [key]: value }));
    setSuccessMessage("");
  }

  function handleSave() {
    const payload = buildPayload(season, form);
    setSuccessMessage("");

    if (calendarExists) {
      updateMutation.mutate(payload, {
        onSuccess: () => {
          setIsNewSeason(false);
          setSuccessMessage("Calendar updated successfully.");
        },
      });
    } else {
      createMutation.mutate(payload, {
        onSuccess: () => {
          setIsNewSeason(false);
          setSuccessMessage("Calendar created successfully.");
        },
      });
    }
  }

  const isSaving = createMutation.isPending || updateMutation.isPending;
  const saveError = createMutation.error || updateMutation.error;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold">League Calendar</h1>
      </div>

      {/* Season Selector */}
      <div className="flex items-center gap-3">
        <label className="text-sm font-medium">Season</label>
        <input
          type="number"
          value={inputSeason}
          onChange={(e) => setInputSeason(e.target.value)}
          min={2020}
          max={2099}
          className="h-9 w-24 rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
        />
        <Button variant="outline" onClick={handleLoad}>
          Load
        </Button>
        <Button variant="outline" onClick={handleNewSeason}>
          New Season
        </Button>
      </div>

      {isLoading && (
        <div className="space-y-4">
          <Skeleton className="h-48" />
          <Skeleton className="h-48" />
          <Skeleton className="h-48" />
        </div>
      )}

      {!isLoading && !calendarExists && !isNewSeason && (
        <div className="rounded-md border p-8 text-center text-muted-foreground">
          No calendar found for season {season}. Click "New Season" to create
          one.
        </div>
      )}

      {(calendarExists || isNewSeason) && !isLoading && (
        <>
          {/* Card 1: Extension Deadlines */}
          <Card>
            <CardHeader>
              <CardTitle>Extension Deadlines</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
                <DateField
                  label={FIELD_LABELS.oext_deadline}
                  value={form.oext_deadline}
                  onChange={(v) => updateField("oext_deadline", v)}
                />
                <DateField
                  label={FIELD_LABELS.iext_window_start}
                  value={form.iext_window_start}
                  onChange={(v) => updateField("iext_window_start", v)}
                />
                <DateField
                  label={FIELD_LABELS.iext_window_end}
                  value={form.iext_window_end}
                  onChange={(v) => updateField("iext_window_end", v)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Card 2: Tag & Tender Deadlines */}
          <Card>
            <CardHeader>
              <CardTitle>Tag & Tender Deadlines</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 sm:grid-cols-2">
                <DateField
                  label={FIELD_LABELS.tag_deadline}
                  value={form.tag_deadline}
                  onChange={(v) => updateField("tag_deadline", v)}
                />
                <DateField
                  label={FIELD_LABELS.tender_deadline}
                  value={form.tender_deadline}
                  onChange={(v) => updateField("tender_deadline", v)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Card 3: Other Deadlines */}
          <Card>
            <CardHeader>
              <CardTitle>Other Deadlines</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
                <DateField
                  label={FIELD_LABELS.br_deadline}
                  value={form.br_deadline}
                  onChange={(v) => updateField("br_deadline", v)}
                />
                <DateField
                  label={FIELD_LABELS.fyo_deadline}
                  value={form.fyo_deadline}
                  onChange={(v) => updateField("fyo_deadline", v)}
                />
                <DateField
                  label={FIELD_LABELS.rookie_signing_deadline}
                  value={form.rookie_signing_deadline}
                  onChange={(v) => updateField("rookie_signing_deadline", v)}
                />
                <DateField
                  label={FIELD_LABELS.salary_rankings_date}
                  value={form.salary_rankings_date}
                  onChange={(v) => updateField("salary_rankings_date", v)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Card 4: Auctions */}
          <Card>
            <CardHeader>
              <CardTitle>Auctions</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-6 lg:grid-cols-2">
                <DateRangeField
                  label="R/F Auction"
                  startValue={form.rf_auction_start}
                  endValue={form.rf_auction_end}
                  onStartChange={(v) => updateField("rf_auction_start", v)}
                  onEndChange={(v) => updateField("rf_auction_end", v)}
                />
                <DateRangeField
                  label="FT Auction"
                  startValue={form.ft_auction_start}
                  endValue={form.ft_auction_end}
                  onStartChange={(v) => updateField("ft_auction_start", v)}
                  onEndChange={(v) => updateField("ft_auction_end", v)}
                />
                <DateRangeField
                  label="RFA Auction"
                  startValue={form.rfa_auction_start}
                  endValue={form.rfa_auction_end}
                  onStartChange={(v) => updateField("rfa_auction_start", v)}
                  onEndChange={(v) => updateField("rfa_auction_end", v)}
                />
                <DateRangeField
                  label="B/R Auction"
                  startValue={form.br_auction_start}
                  endValue={form.br_auction_end}
                  onStartChange={(v) => updateField("br_auction_start", v)}
                  onEndChange={(v) => updateField("br_auction_end", v)}
                />
                <DateRangeField
                  label="Rookie Draft"
                  startValue={form.rookie_draft_start}
                  endValue={form.rookie_draft_end}
                  onStartChange={(v) => updateField("rookie_draft_start", v)}
                  onEndChange={(v) => updateField("rookie_draft_end", v)}
                />
                <DateRangeField
                  label="UDFA Auction"
                  startValue={form.udfa_auction_start}
                  endValue={form.udfa_auction_end}
                  onStartChange={(v) => updateField("udfa_auction_start", v)}
                  onEndChange={(v) => updateField("udfa_auction_end", v)}
                />
                <div className="space-y-1.5">
                  <label className="text-sm font-medium leading-none">
                    UFA Auction
                  </label>
                  <p className="text-xs text-muted-foreground">
                    Start only — UFA auction runs continuously
                  </p>
                  <input
                    type="date"
                    value={form.ufa_auction_start}
                    onChange={(e) =>
                      updateField("ufa_auction_start", e.target.value)
                    }
                    className={dateInputClass}
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Card 5: Season Markers */}
          <Card>
            <CardHeader>
              <CardTitle>Season Markers</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 sm:grid-cols-2">
                <DateField
                  label={FIELD_LABELS.regular_season_start}
                  value={form.regular_season_start}
                  onChange={(v) => updateField("regular_season_start", v)}
                />
                <DateField
                  label={FIELD_LABELS.regular_season_end}
                  value={form.regular_season_end}
                  onChange={(v) => updateField("regular_season_end", v)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Save button + feedback */}
          <div className="flex items-center gap-4">
            <Button onClick={handleSave} disabled={isSaving}>
              {isSaving ? "Saving..." : "Save"}
            </Button>
            {successMessage && (
              <span className="text-sm text-green-600">{successMessage}</span>
            )}
            {saveError && (
              <span className="text-sm text-destructive">
                Error: {saveError.message}
              </span>
            )}
          </div>
        </>
      )}
    </div>
  );
}
