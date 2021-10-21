from utils.catalog import catalog_for_provider


def trigger_post_harvest(provider):
    source = catalog_for_provider(provider)
    post_harvest_script = f"{source.get('post_harvest')}"

    return eval(post_harvest_script)
