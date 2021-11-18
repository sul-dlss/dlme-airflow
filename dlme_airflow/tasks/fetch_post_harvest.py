import subprocess
from utils.catalog import catalog_for_provider


def fetch_post_harvest(provider):
    source = catalog_for_provider(provider)
    post_harvest_script = f"{source.metadata.get('post_harvest')}"

    subprocess.run(post_harvest_script)
