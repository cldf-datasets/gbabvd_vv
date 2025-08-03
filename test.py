from pathlib import Path
import csvw

BASE_DIR = Path(__file__).parent

def test_valid(cldf_dataset, cldf_logger):
    assert cldf_dataset.validate(log=cldf_logger)


def test_correct_sample(cldf_dataset):
    with csvw.UnicodeDictReader(BASE_DIR / 'etc' / 'gb_abvd_map.tsv', delimiter="\t") as reader:
        etc = {o['Grambank_ID']: o['ABVD_ID'] for o in reader}
    
    for o in cldf_dataset['LanguageTable']:
        assert o['ID'] in etc, f"Unexpected in LanguageTable: {o['ID']}"

    for o in cldf_dataset['FormTable']:
        assert o['Language_ID'] in etc, f"Unexpected in FormTable: {o['Language_ID']}"

    for o in cldf_dataset['ValueTable']:
        assert o['Language_ID'] in etc, f"Unexpected in ValueTable: {o['Language_ID']}"


def test_we_dont_have_polynesian(cldf_dataset):
    IGNORE = ['emae1237', 'west2516', 'futu1245', 'mele1250']
    for o in cldf_dataset['LanguageTable']:
        assert o['ID'] not in IGNORE, f"Should not have {o['ID']} in LanguageTable"
    