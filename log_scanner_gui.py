#!/usr/bin/env python3
"""
Ham Radio Log Scanner -- GUI
==============================
Tkinter interface for comparing and deduplicating QSO logs.
Dark theme matching the Log Sync app.
"""
from __future__ import annotations

import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime

from log_scanner import (
    LogSource, ScanReport, DupeGroup, MissingQSO,
    load_source, load_source_from_records,
    fetch_lotw, fetch_clublog, fetch_qrz, fetch_eqsl,
    run_scan, export_adif, generate_report,
)
from log_sync import load_config, save_config

# =============================================================================
#  THEME
# =============================================================================

_C = {
    'bg':       '#1e1e1e',
    'fg':       '#e0e0e0',
    'sel':      '#0078d7',
    'entry':    '#2d2d2d',
    'btn':      '#3c3c3c',
    'head':     '#252526',
    'border':   '#333333',
    'green':    '#44cc44',
    'yellow':   '#cccc44',
    'red':      '#cc4444',
    'dim':      '#888888',
}

_FONT      = ('Segoe UI', 10)
_FONT_SM   = ('Segoe UI', 9)
_FONT_MONO = ('Courier', 9)
_FONT_HDR  = ('Segoe UI', 11, 'bold')


def _auto_name(path: str) -> str:
    base = os.path.splitext(os.path.basename(path))[0]
    lower = base.lower()
    if 'hrd' in lower or 'exportall' in lower:
        return 'HRD'
    if 'jtdx' in lower or 'wsjtx' in lower:
        return 'JTDX'
    if 'lotw' in lower:
        return 'LoTW'
    if 'qrz' in lower:
        return 'QRZ'
    if 'clublog' in lower:
        return 'ClubLog'
    if 'eqsl' in lower:
        return 'eQSL'
    if 'gridtracker' in lower or 'gt2' in lower:
        return 'GT2'
    return base[:20]


# =============================================================================
#  MAIN WINDOW
# =============================================================================

class LogScannerApp(tk.Tk):

    def __init__(self) -> None:
        super().__init__()
        self.title('Ham Radio Log Scanner')
        self.configure(bg=_C['bg'])
        self.geometry('1100x750')
        self.minsize(800, 500)

        self._sources: list[LogSource] = []
        self._report: ScanReport | None = None
        self._scanning = False
        self._cfg = load_config()

        self._build_styles()
        self._build_ui()
        self._load_credentials()

    # -----------------------------------------------------------------
    #  Styles
    # -----------------------------------------------------------------

    def _build_styles(self) -> None:
        style = ttk.Style(self)
        style.theme_use('clam')

        style.configure('.', background=_C['bg'], foreground=_C['fg'], font=_FONT)
        style.configure('TFrame', background=_C['bg'])
        style.configure('TLabel', background=_C['bg'], foreground=_C['fg'], font=_FONT)
        style.configure('TButton', background=_C['btn'], foreground=_C['fg'],
                         font=_FONT, padding=(8, 4))
        style.map('TButton',
                  background=[('active', _C['sel']), ('disabled', _C['bg'])],
                  foreground=[('disabled', _C['dim'])])
        style.configure('TNotebook', background=_C['bg'])
        style.configure('TNotebook.Tab', background=_C['head'], foreground=_C['fg'],
                         font=_FONT, padding=(12, 4))
        style.map('TNotebook.Tab',
                  background=[('selected', _C['sel'])],
                  foreground=[('selected', '#ffffff')])
        style.configure('TLabelframe', background=_C['bg'], foreground=_C['fg'], font=_FONT)
        style.configure('TLabelframe.Label', background=_C['bg'], foreground=_C['fg'])
        style.configure('TEntry', fieldbackground=_C['entry'], foreground=_C['fg'], font=_FONT)
        style.configure('TCheckbutton', background=_C['bg'], foreground=_C['fg'], font=_FONT)

        # Treeview
        style.configure('Treeview',
                         background=_C['bg'], foreground=_C['fg'], fieldbackground=_C['bg'],
                         font=_FONT_MONO, rowheight=22)
        style.configure('Treeview.Heading',
                         background=_C['head'], foreground=_C['fg'], font=_FONT_SM)
        style.map('Treeview', background=[('selected', _C['sel'])])

    # -----------------------------------------------------------------
    #  UI construction
    # -----------------------------------------------------------------

    def _build_ui(self) -> None:
        nb = ttk.Notebook(self)
        nb.pack(fill='both', expand=True, padx=4, pady=4)

        self._build_sources_tab(nb)
        self._build_results_tab(nb)

    def _build_sources_tab(self, nb: ttk.Notebook) -> None:
        tab = ttk.Frame(nb)
        nb.add(tab, text='  Sources  ')

        # -- File list --
        file_frame = ttk.LabelFrame(tab, text='  Log Files  ', padding=8)
        file_frame.pack(fill='both', expand=True, padx=8, pady=(8, 4))

        btn_bar = ttk.Frame(file_frame)
        btn_bar.pack(fill='x', pady=(0, 4))
        ttk.Button(btn_bar, text='Add File...', command=self._add_file).pack(side='left', padx=2)
        ttk.Button(btn_bar, text='Remove Selected', command=self._remove_file).pack(side='left', padx=2)

        cols = ('name', 'path', 'records')
        self._file_tree = ttk.Treeview(file_frame, columns=cols, show='headings', height=6)
        self._file_tree.heading('name', text='Name')
        self._file_tree.heading('path', text='File')
        self._file_tree.heading('records', text='Records')
        self._file_tree.column('name', width=100)
        self._file_tree.column('path', width=500)
        self._file_tree.column('records', width=80, anchor='e')
        self._file_tree.pack(fill='both', expand=True)

        # -- API sources --
        api_frame = ttk.LabelFrame(tab, text='  API Sources  ', padding=8)
        api_frame.pack(fill='x', padx=8, pady=4)

        row = 0
        # LoTW
        self._lotw_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text='LoTW', variable=self._lotw_var).grid(
            row=row, column=0, sticky='w', padx=4)
        ttk.Label(api_frame, text='User:').grid(row=row, column=1, sticky='e', padx=2)
        self._lotw_user = ttk.Entry(api_frame, width=14)
        self._lotw_user.grid(row=row, column=2, padx=2)
        ttk.Label(api_frame, text='Pass:').grid(row=row, column=3, sticky='e', padx=2)
        self._lotw_pass = ttk.Entry(api_frame, width=14, show='*')
        self._lotw_pass.grid(row=row, column=4, padx=2)

        row = 1
        # QRZ
        self._qrz_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text='QRZ', variable=self._qrz_var).grid(
            row=row, column=0, sticky='w', padx=4)
        ttk.Label(api_frame, text='API Key:').grid(row=row, column=1, sticky='e', padx=2)
        self._qrz_key = ttk.Entry(api_frame, width=30, show='*')
        self._qrz_key.grid(row=row, column=2, columnspan=3, sticky='w', padx=2)

        row = 2
        # eQSL
        self._eqsl_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text='eQSL', variable=self._eqsl_var).grid(
            row=row, column=0, sticky='w', padx=4)
        ttk.Label(api_frame, text='User:').grid(row=row, column=1, sticky='e', padx=2)
        self._eqsl_user = ttk.Entry(api_frame, width=14)
        self._eqsl_user.grid(row=row, column=2, padx=2)
        ttk.Label(api_frame, text='Pass:').grid(row=row, column=3, sticky='e', padx=2)
        self._eqsl_pass = ttk.Entry(api_frame, width=14, show='*')
        self._eqsl_pass.grid(row=row, column=4, padx=2)

        row = 3
        # ClubLog
        self._clublog_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text='ClubLog', variable=self._clublog_var).grid(
            row=row, column=0, sticky='w', padx=4)
        ttk.Label(api_frame, text='Call:').grid(row=row, column=1, sticky='e', padx=2)
        self._cl_call = ttk.Entry(api_frame, width=14)
        self._cl_call.grid(row=row, column=2, padx=2)
        ttk.Label(api_frame, text='Email:').grid(row=row, column=3, sticky='e', padx=2)
        self._cl_email = ttk.Entry(api_frame, width=14)
        self._cl_email.grid(row=row, column=4, padx=2)
        ttk.Label(api_frame, text='Pass:').grid(row=row, column=5, sticky='e', padx=2)
        self._cl_pass = ttk.Entry(api_frame, width=14, show='*')
        self._cl_pass.grid(row=row, column=6, padx=2)
        ttk.Label(api_frame, text='API Key:').grid(row=row, column=7, sticky='e', padx=2)
        self._cl_api = ttk.Entry(api_frame, width=14, show='*')
        self._cl_api.grid(row=row, column=8, padx=2)

        # -- Scan controls --
        ctrl_frame = ttk.Frame(tab)
        ctrl_frame.pack(fill='x', padx=8, pady=8)

        ttk.Label(ctrl_frame, text='Mode:').pack(side='left', padx=4)
        self._mode_var = tk.StringVar(value='strict')
        ttk.Radiobutton(ctrl_frame, text='Strict', variable=self._mode_var,
                         value='strict').pack(side='left', padx=2)
        ttk.Radiobutton(ctrl_frame, text='Relaxed', variable=self._mode_var,
                         value='relaxed').pack(side='left', padx=2)

        ttk.Label(ctrl_frame, text='Window (min):').pack(side='left', padx=(16, 4))
        self._window_var = tk.StringVar(value='1')
        ttk.Entry(ctrl_frame, textvariable=self._window_var, width=4).pack(side='left')

        self._scan_btn = ttk.Button(ctrl_frame, text='Scan All', command=self._run_scan)
        self._scan_btn.pack(side='right', padx=8)

        self._status_lbl = ttk.Label(ctrl_frame, text='Ready', foreground=_C['dim'])
        self._status_lbl.pack(side='right', padx=8)

    def _build_results_tab(self, nb: ttk.Notebook) -> None:
        tab = ttk.Frame(nb)
        nb.add(tab, text='  Results  ')

        # -- Summary bar --
        self._summary_lbl = ttk.Label(tab, text='No scan results yet.',
                                       font=_FONT_HDR, foreground=_C['dim'])
        self._summary_lbl.pack(fill='x', padx=8, pady=8)

        # -- Sub-notebook --
        sub_nb = ttk.Notebook(tab)
        sub_nb.pack(fill='both', expand=True, padx=8, pady=(0, 4))

        # Intra dupes tab
        dupe_tab = ttk.Frame(sub_nb)
        sub_nb.add(dupe_tab, text='  Intra-Log Dupes  ')
        dupe_cols = ('source', 'call', 'date', 'band', 'mode', 'times', 'count')
        self._dupe_tree = ttk.Treeview(dupe_tab, columns=dupe_cols, show='headings')
        for col, hdr, w in [('source', 'Source', 80), ('call', 'Call', 100),
                             ('date', 'Date', 90), ('band', 'Band', 60),
                             ('mode', 'Mode', 60), ('times', 'Times', 200),
                             ('count', '#', 40)]:
            self._dupe_tree.heading(col, text=hdr)
            self._dupe_tree.column(col, width=w, anchor='w' if col != 'count' else 'e')
        self._dupe_tree.pack(fill='both', expand=True)

        # Cross gaps tab
        gap_tab = ttk.Frame(sub_nb)
        sub_nb.add(gap_tab, text='  Cross-Log Gaps  ')
        gap_cols = ('call', 'date', 'time', 'band', 'mode', 'present', 'missing')
        self._gap_tree = ttk.Treeview(gap_tab, columns=gap_cols, show='headings')
        for col, hdr, w in [('call', 'Call', 100), ('date', 'Date', 90),
                             ('time', 'Time', 60), ('band', 'Band', 60),
                             ('mode', 'Mode', 60), ('present', 'Present In', 200),
                             ('missing', 'Missing From', 200)]:
            self._gap_tree.heading(col, text=hdr)
            self._gap_tree.column(col, width=w)
        self._gap_tree.pack(fill='both', expand=True)

        # Gap matrix tab
        matrix_tab = ttk.Frame(sub_nb)
        sub_nb.add(matrix_tab, text='  Gap Matrix  ')
        self._matrix_text = tk.Text(matrix_tab, bg=_C['bg'], fg=_C['fg'],
                                     font=_FONT_MONO, state='disabled',
                                     wrap='none', borderwidth=0)
        self._matrix_text.pack(fill='both', expand=True, padx=4, pady=4)

        # -- Bottom bar --
        bot = ttk.Frame(tab)
        bot.pack(fill='x', padx=8, pady=8)
        ttk.Button(bot, text='Export Merged ADIF', command=self._export).pack(side='left', padx=4)
        ttk.Button(bot, text='Copy Report', command=self._copy_report).pack(side='left', padx=4)

    # -----------------------------------------------------------------
    #  Source management
    # -----------------------------------------------------------------

    def _load_credentials(self) -> None:
        """Populate API credential fields from saved config."""
        c = self._cfg
        self._lotw_user.insert(0, c.get('lotw_user', ''))
        self._lotw_pass.insert(0, c.get('lotw_pass', ''))
        self._cl_call.insert(0, c.get('clublog_call', ''))
        self._cl_email.insert(0, c.get('clublog_email', ''))
        self._cl_pass.insert(0, c.get('clublog_pass', ''))
        self._cl_api.insert(0, c.get('clublog_api', ''))
        self._qrz_key.insert(0, c.get('qrz_key', ''))
        self._eqsl_user.insert(0, c.get('eqsl_user', ''))
        self._eqsl_pass.insert(0, c.get('eqsl_pass', ''))
        # Auto-check boxes if credentials exist
        if c.get('lotw_user') and c.get('lotw_pass'):
            self._lotw_var.set(True)
        if c.get('clublog_call') and c.get('clublog_api'):
            self._clublog_var.set(True)
        if c.get('qrz_key'):
            self._qrz_var.set(True)
        if c.get('eqsl_user') and c.get('eqsl_pass'):
            self._eqsl_var.set(True)

    def _save_credentials(self) -> None:
        """Persist API credentials to config file."""
        self._cfg['lotw_user'] = self._lotw_user.get().strip()
        self._cfg['lotw_pass'] = self._lotw_pass.get().strip()
        self._cfg['clublog_call'] = self._cl_call.get().strip()
        self._cfg['clublog_email'] = self._cl_email.get().strip()
        self._cfg['clublog_pass'] = self._cl_pass.get().strip()
        self._cfg['clublog_api'] = self._cl_api.get().strip()
        self._cfg['qrz_key'] = self._qrz_key.get().strip()
        self._cfg['eqsl_user'] = self._eqsl_user.get().strip()
        self._cfg['eqsl_pass'] = self._eqsl_pass.get().strip()
        save_config(self._cfg)

    def _add_file(self) -> None:
        paths = filedialog.askopenfilenames(
            title='Select Log Files',
            filetypes=[('ADIF files', '*.adi *.adif'), ('CSV files', '*.csv'),
                       ('All files', '*.*')],
        )
        for path in paths:
            name = _auto_name(path)
            try:
                src = load_source(name, path)
                self._sources.append(src)
                self._file_tree.insert('', 'end', values=(name, path, src.count))
            except Exception as exc:
                messagebox.showerror('Load Error', f'Failed to load {path}:\n{exc}')

    def _remove_file(self) -> None:
        sel = self._file_tree.selection()
        if not sel:
            return
        for item in sel:
            vals = self._file_tree.item(item, 'values')
            self._sources = [s for s in self._sources if s.origin != vals[1]]
            self._file_tree.delete(item)

    # -----------------------------------------------------------------
    #  Scan
    # -----------------------------------------------------------------

    def _run_scan(self) -> None:
        if self._scanning:
            return
        self._scanning = True
        self._scan_btn.config(state='disabled')
        self._status_lbl.config(text='Scanning...', foreground=_C['yellow'])

        threading.Thread(target=self._scan_worker, daemon=True).start()

    def _scan_worker(self) -> None:
        try:
            # Save credentials before scanning
            self.after(0, self._save_credentials)

            sources = list(self._sources)

            # Fetch API sources
            if self._lotw_var.get():
                user = self._lotw_user.get().strip()
                pw = self._lotw_pass.get().strip()
                if user and pw:
                    self.after(0, lambda: self._status_lbl.config(text='Fetching LoTW...'))
                    records = fetch_lotw(user, pw)
                    src = load_source_from_records('LoTW', 'api:lotw', records)
                    sources.append(src)
                    self.after(0, lambda n=src.count: self._file_tree.insert(
                        '', 'end', values=('LoTW', 'api:lotw', n)))

            if self._qrz_var.get():
                key = self._qrz_key.get().strip()
                if key:
                    self.after(0, lambda: self._status_lbl.config(text='Fetching QRZ...'))
                    records = fetch_qrz(key)
                    src = load_source_from_records('QRZ', 'api:qrz', records)
                    sources.append(src)
                    self.after(0, lambda n=src.count: self._file_tree.insert(
                        '', 'end', values=('QRZ', 'api:qrz', n)))

            if self._eqsl_var.get():
                user = self._eqsl_user.get().strip()
                pw = self._eqsl_pass.get().strip()
                if user and pw:
                    self.after(0, lambda: self._status_lbl.config(text='Fetching eQSL...'))
                    records = fetch_eqsl(user, pw)
                    src = load_source_from_records('eQSL', 'api:eqsl', records)
                    sources.append(src)
                    self.after(0, lambda n=src.count: self._file_tree.insert(
                        '', 'end', values=('eQSL', 'api:eqsl', n)))

            if self._clublog_var.get():
                call = self._cl_call.get().strip()
                email = self._cl_email.get().strip()
                pw = self._cl_pass.get().strip()
                api = self._cl_api.get().strip()
                if call and email and pw and api:
                    self.after(0, lambda: self._status_lbl.config(text='Fetching ClubLog...'))
                    records = fetch_clublog(call, email, pw, api)
                    src = load_source_from_records('ClubLog', 'api:clublog', records)
                    sources.append(src)
                    self.after(0, lambda n=src.count: self._file_tree.insert(
                        '', 'end', values=('ClubLog', 'api:clublog', n)))

            mode = self._mode_var.get()
            try:
                window = int(self._window_var.get())
            except ValueError:
                window = 1

            self.after(0, lambda: self._status_lbl.config(
                text=f'Comparing {len(sources)} sources...'))

            report = run_scan(sources, mode=mode, window_min=window)
            self._report = report
            self.after(0, lambda: self._display_results(report))

        except Exception as exc:
            self.after(0, lambda e=str(exc): self._status_lbl.config(
                text=f'Error: {e}', foreground=_C['red']))
        finally:
            self._scanning = False
            self.after(0, lambda: self._scan_btn.config(state='normal'))

    # -----------------------------------------------------------------
    #  Display results
    # -----------------------------------------------------------------

    def _display_results(self, report: ScanReport) -> None:
        total_dupes = sum(
            sum(len(dg.records) - 1 for dg in dupes)
            for dupes in report.intra_dupes.values()
        )
        self._summary_lbl.config(
            text=(f'Sources: {len(report.sources)}    '
                  f'Unique QSOs: {len(report.master_records)}    '
                  f'Dupes: {total_dupes}    '
                  f'Gaps: {len(report.missing)}'),
            foreground=_C['green'] if not report.missing else _C['yellow'],
        )
        self._status_lbl.config(text='Scan complete', foreground=_C['green'])

        # -- Intra dupes --
        self._dupe_tree.delete(*self._dupe_tree.get_children())
        for src_name, dupes in report.intra_dupes.items():
            for dg in dupes:
                rec = dg.records[0]
                times = ', '.join(r.get('TIME_ON', '?') for r in dg.records)
                self._dupe_tree.insert('', 'end', values=(
                    src_name,
                    rec.get('CALL', '?'),
                    rec.get('QSO_DATE', '?'),
                    rec.get('BAND', '?'),
                    rec.get('MODE', '?'),
                    times,
                    len(dg.records),
                ))

        # -- Cross gaps --
        self._gap_tree.delete(*self._gap_tree.get_children())
        for mq in report.missing:
            rec = mq.record
            self._gap_tree.insert('', 'end', values=(
                rec.get('CALL', '?'),
                rec.get('QSO_DATE', '?'),
                rec.get('TIME_ON', '?'),
                rec.get('BAND', '?'),
                rec.get('MODE', '?'),
                ', '.join(mq.present_in),
                ', '.join(mq.missing_from),
            ))

        # -- Gap matrix --
        self._matrix_text.config(state='normal')
        self._matrix_text.delete('1.0', 'end')
        if report.missing:
            from collections import defaultdict
            src_names = [s.name for s in report.sources]
            gap_matrix: dict[tuple[str, str], int] = defaultdict(int)
            for m in report.missing:
                for mn in m.missing_from:
                    for pn in m.present_in:
                        gap_matrix[(pn, mn)] += 1

            col_w = max(len(n) for n in src_names) + 2
            header = f'{"Missing from ->":>{col_w}}'
            for name in src_names:
                header += f'  {name:>{col_w}}'
            self._matrix_text.insert('end', header + '\n')
            self._matrix_text.insert('end', '-' * len(header) + '\n')

            for present in src_names:
                row = f'{"In " + present:>{col_w}}'
                for missing in src_names:
                    if present == missing:
                        row += f'  {"--":>{col_w}}'
                    else:
                        count = gap_matrix.get((present, missing), 0)
                        row += f'  {count:>{col_w}d}'
                self._matrix_text.insert('end', row + '\n')
        else:
            self._matrix_text.insert('end', 'All sources are in sync.')
        self._matrix_text.config(state='disabled')

    # -----------------------------------------------------------------
    #  Actions
    # -----------------------------------------------------------------

    def _export(self) -> None:
        if not self._report:
            messagebox.showinfo('No Data', 'Run a scan first.')
            return
        path = filedialog.asksaveasfilename(
            title='Export Merged ADIF',
            defaultextension='.adi',
            filetypes=[('ADIF files', '*.adi *.adif'), ('All files', '*.*')],
        )
        if not path:
            return
        try:
            n = export_adif(self._report.master_records, path)
            messagebox.showinfo('Export Complete', f'Exported {n} records to:\n{path}')
        except Exception as exc:
            messagebox.showerror('Export Error', str(exc))

    def _copy_report(self) -> None:
        if not self._report:
            messagebox.showinfo('No Data', 'Run a scan first.')
            return
        text = generate_report(self._report)
        self.clipboard_clear()
        self.clipboard_append(text)
        self._status_lbl.config(text='Report copied to clipboard', foreground=_C['green'])


# =============================================================================
#  ENTRY POINT
# =============================================================================

def main() -> None:
    app = LogScannerApp()
    app.mainloop()


if __name__ == '__main__':
    main()
