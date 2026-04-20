def classFactory(iface):
    from .ghg_mapper_plugin import GHGMapperPlugin
    return GHGMapperPlugin(iface)
