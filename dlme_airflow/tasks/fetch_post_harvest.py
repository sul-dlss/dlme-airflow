from utils.catalog import catalog_for_provider


def fetch_post_harvest(provider):
    source = catalog_for_provider(provider)
    post_harvest_script = f"{source.metadata.get('post_harvest')}"

    with open(post_harvest_script, "rb") as source_file:
        code = compile(source_file.read(), post_harvest_script, "exec")
    exec(code)
