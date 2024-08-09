import json
from setuptools import setup

with open("metadata.json", encoding="utf-8") as fp:
    metadata = json.load(fp)

setup(
    name='cldfbench_gbabvd_vv',
    description=metadata["title"],
    license=metadata.get("license", ""),
    url=metadata.get("url", ""),
    py_modules=['cldfbench_gbabvd_vv'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'gbabvd_vv=cldfbench_gbabvd_vv:Dataset',
        ],
        "cldfbench.commands": [
            "gbabvd_vv=gbabvd_vv_subcommands",
        ]
    },
    install_requires=[
        'cldfbench',
        'pycldf',
        'clldutils',
        'gitdb',
        'cldfzenodo',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
