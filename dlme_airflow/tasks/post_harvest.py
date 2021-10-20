from utils.catalog import catalog_for_provider


def post_harvest_t(provider):
    source = catalog_for_provider(provider)
    post_harvest_script = f"{source.get('post_harvest')}"

    return post_harvest_script # need to call code that needs to run instead of returning post_harvest_script
