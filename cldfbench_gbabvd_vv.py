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


# ignore these words as they are hard to identify cognates in vanuatu (& elsewhere)
# following discussions with MR/BE/MW
ABVD_PARAMETERS_TO_IGNORE = [
    '8_toturn',
    '10_dirty',
    '78_tocuthack',
    '152_small',
    '158_narrow',
    '159_wide',
    '174_ininside',
    '185_we',
    '190_other',
    '191_all',
    '193_if',
    '202_six',
    '203_seven',
    '204_eight',
    '205_nine',
    '206_ten',
    '207_twenty',
    '208_fifty',
    '209_onehundred',
]



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
        wanted = {
            w['Grambank_ID']: w['ABVD_ID'] for w in 
            self.etc_dir.read_csv("gb_abvd_map.tsv", delimiter="\t", dicts=True)
        }
        wanted_abvd = {v: k for k, v in wanted.items()}  # inverse lookup
        
        abvd = pycldf.Dataset.from_metadata('./raw/abvd/cldf/cldf-metadata.json')
        gb = pycldf.Dataset.from_metadata('./raw/grambank/cldf/StructureDataset-metadata.json')

        # for lg in abvd.objects('LanguageTable'):
        # for lg in gb.objects('LanguageTable'):
        #
        #
        # abvd_lgs = {}
        # abvd_ids = set()
        # seen_gcs = set()
        # for lg in abvd.objects('LanguageTable'):
        #     if lg.cldf.id in gb_abvd_map:
        #         abvd_lgs[lg.cldf.id] = lg
        #         abvd_ids.add(lg.cldf.id)
        #         seen_gcs.add(lg.cldf.glottocode)
        #
        with args.writer as ds:
            ds.cldf.add_component('ParameterTable')
            ds.cldf.add_component('LanguageTable')
            ds.cldf.add_component('FormTable')
            ds.cldf.add_component('CognateTable')
            ds.cldf.add_columns('LanguageTable', 'ABVD_ID')
            ds.cldf.add_columns('ParameterTable', 'Concepticon_ID')
            ds.cldf.add_columns('FormTable', 'Cognacy')
            ds.cldf.add_columns('FormTable', 'Loan')
            ds.cldf.add_columns('CognateTable', 'Doubt')

            ds.cldf.add_sources(Path.read_text(self.etc_dir / 'sources.bib'))
            
            # add languages
            for lg in gb.objects('LanguageTable'):
                glottocode = lg.cldf.id
                if glottocode in wanted:
                    ds.objects['LanguageTable'].append({
                        'ID': glottocode,
                        'ABVD_ID': wanted[glottocode],
                        'Name': f'{lg.cldf.name}',
                        'Macroarea': lg.cldf.macroarea,
                        'Glottocode': glottocode,
                        'Latitude': lg.cldf.latitude,
                        'Longitude': lg.cldf.longitude,
                    })
            ds.objects['LanguageTable'].sort(key=lambda d: d['ID'])
            
            # add grambank parameters
            for p in gb.objects('ParameterTable'):
                ds.objects['ParameterTable'].append({
                    'ID': p.cldf.id,
                    'Name': p.cldf.name,
                })
            
            # add grambank data
            for v in gb.objects('ValueTable'):
                if v.cldf.languageReference in wanted:
                    ds.objects['ValueTable'].append({
                        'ID': v.cldf.id,
                        'Language_ID': v.cldf.languageReference,
                        'Parameter_ID': v.cldf.parameterReference,
                        'Value': v.cldf.value,
                        'Source': ['Skirgardetal2023'],
                    })
            
            # add ABVD parameters
            badword_ids = set()
            for p in abvd.objects('ParameterTable'):
                if p.cldf.id not in ABVD_PARAMETERS_TO_IGNORE:
                    ds.objects['ParameterTable'].append({
                        'ID': p.cldf.id,
                        'Name': p.cldf.name,
                        'Concepticon_ID': p.cldf.concepticonReference
                    })
                else:
                    print(f"Ignoring bad word - {p.cldf.id}: {p.cldf.name}")
                    badword_ids.add(p.cldf.id)

            # add abvd lexicon items
            prf = Profile.from_file(self.etc_dir / 'orthography.tsv', form='NFC')
            tok = Tokenizer(profile=prf)
            ignore = ['..']
            
            seen_form_ids = {}
            for form in abvd.objects('FormTable'):
                abvd_id = form.cldf.languageReference
                param_id = form.cldf.parameterReference
                
                if form.cldf.form in ignore:
                    print(f"WTF {abvd_id} - {form.cldf.form}")
                
                if abvd_id in wanted_abvd and form.cldf.form not in ignore and param_id not in badword_ids:
                    frm = unicodedata.normalize('NFC', form.cldf.form)
                    ds.objects['FormTable'].append({
                        'ID': form.id,
                        'Language_ID': wanted_abvd[abvd_id],  # use glottocode as language ID
                        'Parameter_ID': param_id,
                        'Value': form.cldf.value,
                        'Form': frm,
                        'Segments': tok(frm, column='IPA', form='NFC').split(' '),
                        'Source': ['Greenhilletal2008'],
                        'Cognacy': form.data['Cognacy'],
                        'Loan': form.data['Loan'],
                    })
                    seen_form_ids[form.id] = form.data['Cognacy']
            
            # finally add cognates
            for c in abvd.objects('CognateTable'):
                form_id = c.data['Form_ID']
                if form_id in seen_form_ids:
                    ds.objects['CognateTable'].append({
                        'ID': c.id,
                        'Form_ID': form_id,
                        'Cognateset_ID': c.data['Cognateset_ID'],
                        'Cognacy': seen_form_ids[form_id],
                        'Doubt': c.data['Doubt'],
                        'Source': ['Greenhilletal2008'],
                    })

            ds.objects['ParameterTable'].sort(key=lambda d: d['ID'])
            ds.objects['ValueTable'].sort(key=lambda r: (r['Language_ID'], r['Parameter_ID']))
            ds.objects['FormTable'].sort(key=lambda r: (r['Language_ID'], r['Parameter_ID']))
            ds.objects['CognateTable'].sort(key=lambda d: d['Cognateset_ID'])
