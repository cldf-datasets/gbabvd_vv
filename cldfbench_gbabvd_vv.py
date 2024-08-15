"""Run:
cldfbench makecldf --with-zenodo --with-cldfreadme --communities lexibank ./cldfbench_gbabvd_vv.py
cldfbench readme ./cldfbench_gbabvd_vv.py
cldfbench gbabvd_vv.gbabvdvv_analyse
"""

from pathlib import Path
from collections import defaultdict
from git import Repo, GitCommandError
import unicodedata

import pycldf

from clldutils.path import read_text

from cldfzenodo import oai_lexibank
from cldfzenodo.record import GithubRepos

from cldfbench import Dataset as BaseDataset
from cldfbench import CLDFSpec, Metadata

from segments import Profile, Tokenizer


class Dataset(BaseDataset):
    dir = Path(__file__).parent
    id = "gbabvd_vv"

    def cldf_specs(self):
        return CLDFSpec(
            module='StructureDataset',
            dir=self.cldf_dir,
            metadata_fname='cldf-metadata.json'
        )

    def cmd_download(self, args):
        self.dataset_meta = {
            r["ID"]: r["URL"]
            for r in self.etc_dir.read_csv("datasets.tsv", delimiter="\t", dicts=True)
        }

        github_info = {rec.doi: rec.github_repos for rec in oai_lexibank()}

        for dataset, src in self.dataset_meta.items():
            ghinfo = github_info[src] if src in github_info else GithubRepos.from_url(src)
            args.log.info("Checking {}".format(dataset))
            dest = self.raw_dir / dataset

            # download data
            if dest.exists():
                args.log.info("... dataset already exists, pulling changes")
                for remote in Repo(str(dest)).remotes:
                    remote.fetch()
            else:
                args.log.info("... cloning {}".format(dataset))
                try:
                    Repo.clone_from(ghinfo.clone_url, str(dest))
                except GitCommandError as e:
                    args.log.error("... download failed\n{}".format(str(e)))
                    continue

            # check out release (fall back to master branch)
            repo = Repo(str(dest))
            if ghinfo.tag:
                args.log.info("... checking out tag {}".format(ghinfo.tag))
                repo.git.checkout(ghinfo.tag)
            else:
                args.log.warning("... could not determine tag to check out")
                args.log.info("... checking out master")
                try:
                    branch = repo.branches.main
                    branch.checkout()
                except AttributeError:
                    try:
                        branch = repo.branches.master
                        branch.checkout()
                    except AttributeError:
                        args.log.error("found neither main nor master branch")
                repo.git.merge()

    def cmd_makecldf(self, args):
        vv_island_north = (-12.829882, 166.507220)  # (-15.871027018941662, 167.2265488760759)
        vv_island_west = (-16.232479, 166.111713)  # (-16.083418602140625, 167.13775323375486)
        vv_island_east = (-16.650297, 170.477881)  # (-16.46849476280046, 167.86200219008973)
        vv_island_south = (-20.551360, 168.988437)  # (-16.624889040314066, 167.5021108694064)

        # abvd_id <-> gb_glottocode
        gb_abvd_map = {}
        for r in self.etc_dir.read_csv("gb_abvd_map.tsv", delimiter="\t", dicts=True):
            gb_abvd_map[r["Grambank_ID"]] = r["ABVD_ID"]
            gb_abvd_map[r["ABVD_ID"]] = r["Grambank_ID"]

        abvd = pycldf.Dataset.from_metadata('./raw/abvd/cldf/cldf-metadata.json')
        gb = pycldf.Dataset.from_metadata('./raw/grambank/cldf/StructureDataset-metadata.json')

        abvd_gb_map = defaultdict(set)
        for lg in abvd.objects('LanguageTable'):
            abvd_gb_map[lg.cldf.glottocode].add(lg.cldf.id)
            abvd_gb_map[lg.cldf.id].add(lg.cldf.glottocode)

        gb_lgs = {}
        for lg in gb.objects('LanguageTable'):
            if lg.cldf.latitude is not None and \
                    lg.cldf.latitude < vv_island_north[0] and lg.cldf.latitude > vv_island_south[0] and \
                    lg.cldf.longitude > vv_island_west[1] and lg.cldf.longitude < vv_island_east[1]:
                if lg.cldf.glottocode in gb_abvd_map:
                    gb_lgs[lg.cldf.glottocode] = lg
                elif len(abvd_gb_map[lg.cldf.glottocode]) == 1:
                    gb_lgs[lg.cldf.glottocode] = lg
                    a_id = list(abvd_gb_map[lg.cldf.glottocode])[0]
                    gb_abvd_map[lg.cldf.glottocode] = a_id
                    gb_abvd_map[a_id] = lg.cldf.glottocode

        abvd_lgs = {}
        abvd_ids = set()
        seen_gcs = set()
        for lg in abvd.objects('LanguageTable'):
            if lg.cldf.id in gb_abvd_map:
                if lg.cldf.glottocode in seen_gcs:
                    continue
                abvd_lgs[lg.cldf.id] = lg
                abvd_ids.add(lg.cldf.id)
                seen_gcs.add(lg.cldf.glottocode)

        with args.writer as ds:
            ds.cldf.add_component('ParameterTable')
            ds.cldf.add_component('LanguageTable')
            ds.cldf.add_component('FormTable')
            ds.cldf.add_component('CognateTable')
            ds.cldf.add_columns('LanguageTable', 'ABVD_ID')
            ds.cldf.add_columns('FormTable', 'Cognacy')
            ds.cldf.add_columns('FormTable', 'Loan')
            ds.cldf.add_columns('CognateTable', 'Doubt')

            ds.cldf.add_sources(Path.read_text(self.etc_dir / 'sources.bib'))

            for g, lg in gb_lgs.items():
                ds.objects['LanguageTable'].append({
                    'ID': lg.cldf.id,
                    'ABVD_ID': abvd_lgs[gb_abvd_map[g]].cldf.id,
                    'Name': f'{lg.cldf.name}/{abvd_lgs[gb_abvd_map[g]].cldf.name}',
                    'Macroarea': lg.cldf.macroarea,
                    'Glottocode': g,
                    'Latitude': lg.cldf.latitude,
                    'Longitude': lg.cldf.longitude,
                })
            ds.objects['LanguageTable'].sort(key=lambda d: d['ID'])

            seen_params = set()
            for v in gb.objects('ValueTable'):
                if v.cldf.languageReference in gb_lgs:
                    if v.cldf.parameterReference not in seen_params:
                        p = gb.objects('ParameterTable')[v.cldf.parameterReference]
                        ds.objects['ParameterTable'].append({
                            'ID': p.cldf.id,
                            'Name': p.cldf.name,
                        })
                        seen_params.add(v.cldf.parameterReference)
                    ds.objects['ValueTable'].append({
                        'ID': v.cldf.id,
                        'Language_ID': v.cldf.languageReference,
                        'Parameter_ID': v.cldf.parameterReference,
                        'Value': v.cldf.value,
                        'Source': ['Skirgardetal2023'],
                    })

            seen_params = set()
            seen_form_ids = set()

            prf = Profile.from_file(self.etc_dir / 'orthography.tsv', form='NFC')
            tok = Tokenizer(profile=prf)
            ign = ['..']
            seen_fids = set()
            for form in abvd.objects('FormTable'):
                if form.cldf.languageReference in abvd_ids:
                    if form.cldf.form in ign:
                        continue
                    if form.cldf.parameterReference not in seen_params:
                        p = abvd.objects('ParameterTable')[form.cldf.parameterReference]
                        ds.objects['ParameterTable'].append({
                            'ID': p.cldf.id,
                            'Name': p.cldf.name,
                        })
                        seen_params.add(form.cldf.parameterReference)
                    frm = unicodedata.normalize('NFC', form.cldf.form)
                    if frm == 'naᵐ batina':
                        frm = 'na ᵐbatina'
                    lid = gb_abvd_map[form.cldf.languageReference]
                    fid = f'{lid}-{form.cldf.parameterReference}'
                    fid_cnt = 1
                    while f'{fid}-{fid_cnt}' in seen_fids:
                        fid_cnt += 1
                    nfid = f'{fid}-{fid_cnt}'
                    seen_fids.add(nfid)
                    ds.objects['FormTable'].append({
                        'ID': nfid,
                        'Language_ID': lid,
                        'Parameter_ID': form.cldf.parameterReference,
                        'Value': form.cldf.value,
                        'Form': frm,
                        'Segments': tok(frm, column='IPA', form='NFC').split(' '),
                        'Source': ['Greenhilletal2008'],
                        'Cognacy': form.data['Cognacy'],
                        'Loan': form.data['Loan'],
                    })
                    seen_form_ids.add(nfid)

            for c in abvd.objects('CognateTable'):
                fids = c.data['Form_ID'].split('-')
                if fids[0] not in gb_abvd_map:
                    continue
                fids[0] = gb_abvd_map[fids[0]]
                fid = '-'.join(fids)
                if fid in seen_form_ids:
                    ds.objects['CognateTable'].append({
                        'ID': fid,
                        'Form_ID': fid,
                        'Cognateset_ID': c.data['Cognateset_ID'],
                        'Doubt': c.data['Doubt'],
                        'Source': ['Greenhilletal2008'],
                    })

            ds.objects['ParameterTable'].sort(key=lambda d: d['ID'])
            ds.objects['CognateTable'].sort(key=lambda d: d['Cognateset_ID'])
            ds.objects['ValueTable'].sort(key=lambda r: (r['Language_ID'], r['Parameter_ID']))
            ds.objects['FormTable'].sort(key=lambda r: (r['Language_ID'], r['Parameter_ID']))
