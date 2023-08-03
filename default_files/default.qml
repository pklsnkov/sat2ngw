<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.16.16-Hannover" maxScale="0" hasScaleBasedVisibilityFlag="0" styleCategories="AllStyleCategories" minScale="1e+08">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
  </flags>
  <temporal enabled="0" mode="0" fetchMode="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <customproperties>
    <property key="WMSBackgroundLayer" value="false"/>
    <property key="WMSPublishDataSourceUrl" value="false"/>
    <property key="embeddedWidgets/count" value="0"/>
    <property key="identify/format" value="Value"/>
  </customproperties>
  <pipe>
    <provider>
      <resampling enabled="false" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour" maxOversampling="2"/>
    </provider>
    <rasterrenderer opacity="1" alphaBand="-1" grayBand="1" nodataColor="" type="singlebandgray" gradient="BlackToWhite">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>CumulativeCut</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.05</cumulativeCutLower>
        <cumulativeCutUpper>0.95</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <contrastEnhancement>
        <minValue></minValue>
        <maxValue></maxValue>
        <algorithm>StretchToMinimumMaximum</algorithm>
      </contrastEnhancement>
    </rasterrenderer>
    <brightnesscontrast brightness="0" gamma="1" contrast="0"/>
    <huesaturation colorizeOn="0" colorizeStrength="100" colorizeRed="255" saturation="0" grayscaleMode="0" colorizeGreen="128" colorizeBlue="128"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
