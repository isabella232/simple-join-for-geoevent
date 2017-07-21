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
		return "10.4.0";
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
