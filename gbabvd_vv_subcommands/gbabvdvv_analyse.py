"""
Output a matrix properties by languages.

"""

from collections import defaultdict
from itertools import groupby

from pycldf import Dataset

from cldfbench_gbabvd_vv import Dataset

NA_MARKER = '?'


def all_equal(iterable):
    g = groupby(iterable)
    return next(g, True) and not next(g, False)


def run(args):
    ds = Dataset().cldf_reader()

    form_ids = {}
    for fm in ds['FormTable']:
        form_ids[fm['ID']] = fm

    abvd_lg_ids = {}
    abvd_gc_ids = {}
    for fm in ds['LanguageTable']:
        abvd_lg_ids[fm['ABVD_ID']] = fm
        abvd_gc_ids[fm['Glottocode']] = fm['ABVD_ID']

    lids = {}
    pids = {}
    for c in ds['CognateTable']:
        fid = c['Form_ID']
        lid = form_ids[fid]['Language_ID']
        pids[c['Cognateset_ID']] = form_ids[fid]['Parameter_ID']
        lids[c['ID']] = abvd_lg_ids[lid]['ID']

    gb_params = [p['ID'] for p in ds['ParameterTable'] if p['ID'].startswith('GB')]
    abvd_cogset_ids = sorted(list(set([c['Cognateset_ID'] for c in ds['CognateTable']])))

    abvd_forms = {}
    for f in ds['FormTable']:
        abvd_forms[f['ID']] = f['Form']

    data = defaultdict(dict)
    data_lex = defaultdict(dict)

    for fm in ds['LanguageTable']:
        gc = fm['Glottocode']
        for c in ds['ValueTable']:
            if c['Language_ID'] == gc:
                data[gc][c['Parameter_ID']] = c['Value']
        for c in ds['CognateTable']:
            if lids[c['ID']] == gc:
                data[gc][c['Cognateset_ID']] = f'1|{abvd_forms[c["Form_ID"]]}'

    out_data = defaultdict(list)
    for fm in ds['LanguageTable']:
        gc = fm['Glottocode']
        for gp in gb_params:
            if gp in data[gc]:
                d = data[gc][gp]
                if d is not None:
                    if d == '?':
                        d = NA_MARKER
                    out_data[gc].append(d)
                else:
                    out_data[gc].append(NA_MARKER)
            else:
                out_data[gc].append(NA_MARKER)

        for ap in abvd_cogset_ids:
            if ap in data[gc]:
                out_data[gc].append(data[gc][ap])
            else:
                pid = pids[ap]
                o = '?'
                fs = []
                for f in form_ids.values():
                    # find at least one other lexeme which is not set to the current cognate
                    if f['Parameter_ID'] == pid and f['Language_ID'] == abvd_gc_ids[gc]:
                        fs.append(f['Form'])
                if len(fs) == 0:
                    out_data[gc].append(NA_MARKER)
                else:
                    out_data[gc].append('0')

    header = gb_params + abvd_cogset_ids

    ignore_properties = set()
    for i in range(len(header)):
        col_data = []
        for g, v in out_data.items():
            col_data.append(v[i].split('|')[0])
        if all_equal(col_data):
            ignore_properties.add(i)

    out_data2 = {}
    header2 = []
    first_run = True
    for g, d in out_data.items():
        cleaned_col = []
        for i, d_ in enumerate(d):
            if i not in ignore_properties:
                cleaned_col.append(d_)
                if first_run:
                    header2.append(header[i])
        first_run = False
        out_data2[g] = cleaned_col

    header3 = [''] + header2
    print(",".join(header3))
    for g, d in out_data2.items():
        print(f'{g},{",".join(d)}')
