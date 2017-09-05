/*
  Copyright 2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.processor.simplejoin;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class SimpleJoinDefinition extends GeoEventProcessorDefinitionBase
{
  private static final BundleLogger LOGGER = BundleLoggerFactory.getLogger(SimpleJoinDefinition.class);

  public SimpleJoinDefinition()
  {
    try
    {
      propertyDefinitions.put("sourceGeoEventDef", new PropertyDefinition("sourceGeoEventDef", PropertyType.String, "SourceDef", "Source GeoEvent Definition Name", "Source GeoEvent Defintion", false, false));
      propertyDefinitions.put("joinGeoEventDef", new PropertyDefinition("joinGeoEventDef", PropertyType.String, "JoinDef", "Join GeoEvent Definition Name", "Join GeoEvent Defintion", false, false));
      propertyDefinitions.put("sourceKeyField", new PropertyDefinition("sourceKeyField", PropertyType.String, "TRACK_ID", "Source Key Field Name", "Source Key Field Name", true, false));
      propertyDefinitions.put("joinKeyField", new PropertyDefinition("joinKeyField", PropertyType.String, "TRACK_ID", "Join Key Field Name", "Join Key Field Name", true, false));
      propertyDefinitions.put("clearCacheOnStart", new PropertyDefinition("clearCacheOnStart", PropertyType.Boolean, false, "Clear Cache on Start", "Clear Cache on Start", true, false));
      propertyDefinitions.put("newGeoEventDefinitionName", new PropertyDefinition("newGeoEventDefinitionName", PropertyType.String, "SimpleJoinDefinition", "Resulting GeoEvent Definition Name", "SimpleJoin", false, false));
    }
    catch (Exception error)
    {
      LOGGER.error("INIT_ERROR", error.getMessage());
      LOGGER.info(error.getMessage(), error);
    }
  }

  @Override
  public String getName()
  {
    return "SimpleJoin";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.processor";
  }

  @Override
  public String getVersion()
  {
    return "10.5.0";
  }

  @Override
  public String getLabel()
  {
    return "${com.esri.geoevent.processor.simple-join-processor.PROCESSOR_LABEL}";
  }

  @Override
  public String getDescription()
  {
    return "${com.esri.geoevent.processor.simple-join-processor.PROCESSOR_DESC}";
  }
}
