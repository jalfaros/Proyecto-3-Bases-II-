from PyQt5.QtCore import *
from qgis.core import *
import qgis.utils
import psycopg2


vl = QgsVectorLayer("POINT?crs=EPSG:4326", "Empresas", "memory")
# Instanciación del proveedor de datos de la nueva capa
pr = vl.dataProvider()
# add fields
pr.addAttributes( [ QgsField("nombre", QVariant.String)] )
# Instanciación del gruporaíz del arbol de capas
layerTree = iface.layerTreeCanvasBridge().rootGroup()

# Inserción de la nueva capa en la pocisión 0 del panel de capas
layerTree.insertChildNode(0, QgsLayerTreeLayer(vl))
canvas = qgis.utils.iface.mapCanvas()

# Función a ejecutar por el hilo
def ver_datos(nombre, tabla):
    conn = psycopg2.connect(
        host="leoviquez.com",
        database="p3_empresas",
        user="basesII",
        password="12345")
    cur = conn.cursor()
    cur.execute("select nombre,st_astext(geom) from todas_empresas where ("+tabla+"->>'tipo')='"+nombre+"'")
    empresas = cur.fetchall() 
    vl.startEditing()
    for row in empresas:
        feature = QgsFeature()
        feature.setGeometry( QgsGeometry.fromWkt(row[1]))
        feature.setAttributes([row[0]])
        # Inicia edición agrega la geometría y acepta los cambios
        pr.addFeatures( [feature] )
    vl.commitChanges()
    # Actualiza la extensión de la nueva capa 
    vl.updateExtents() 
    canvas.setExtent(vl.extent())


    
ver_datos("COMERCIAL", "detalles")