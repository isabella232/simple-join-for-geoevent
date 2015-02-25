package com.esri.geoevent.processor.simplejoin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import com.esri.ges.core.Uri;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.FieldCardinality;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;
import com.esri.ges.util.Converter;

public class SimpleJoin extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable, ServiceTrackerCustomizer
{
	private static final BundleLogger							LOGGER			= BundleLoggerFactory.getLogger(SimpleJoin.class);

	private String	sourceGeoEventDef;
	private String	joinGeoEventDef;
	private String	sourceKeyField;
	private String	joinKeyField;
    private String  geoEventDefinitionName;

	private final Map<String, GeoEvent>							joinTrackCache		= new ConcurrentHashMap<String, GeoEvent>();
	private Map<String, String>									edMapper	= new ConcurrentHashMap<String, String>();
    private ServiceTracker            							geoEventDefinitionManagerTracker;
	private GeoEventDefinitionManager 							geoEventDefinitionManager;

	private Messaging											messaging;
	private GeoEventCreator										geoEventCreator;
	private GeoEventProducer									geoEventProducer;
	private Date												clearCacheTime;
	private boolean												autoClearCache;
	private boolean												clearCacheOnStart;
	private Timer												clearCacheTimer;
	private String												categoryField;
	private Uri													definitionUri;
	private String												definitionUriString;
	private boolean												isIterating		= false;
	final Object												lock1			= new Object();

	class ClearCacheTask extends TimerTask
	{
		public void run()
		{
			// clear the cache
			if (autoClearCache == true)
			{
				joinTrackCache.clear();
			}
		}
	}

	class CacheIterator implements Runnable
	{
		private Long	cycleInterval	= 5000L;
		private Long	messageInterval = 1L;

		public CacheIterator(String category, Long cycleInterval, Long messageInterval)
		{
			this.cycleInterval = cycleInterval;
			this.messageInterval = messageInterval;
		}

		@Override
		public void run()
		{
			while (isIterating)
			{
				try
				{
					Thread.sleep(cycleInterval);

					for (String catId : joinTrackCache.keySet())
					{
						GeoEvent geoEvent = joinTrackCache.get(catId);
						try
						{
							createGeoEventAndSend(geoEvent);
							Thread.sleep(messageInterval);
						}
						catch (MessagingException error)
						{
							LOGGER.error("SEND_ERROR", catId, error.getMessage());
							LOGGER.info(error.getMessage(), error);
						}
					}
				}
				catch (InterruptedException error)
				{
					LOGGER.error(error.getMessage(), error);
				}
			}
		}
	}

	protected SimpleJoin(GeoEventProcessorDefinition definition) throws ComponentException
	{
	    super(definition);
	    if (geoEventDefinitionManagerTracker == null)
	      geoEventDefinitionManagerTracker = new ServiceTracker(definition.getBundleContext(), GeoEventDefinitionManager.class.getName(), this);
	    geoEventDefinitionManagerTracker.open();
		
	}

	public void afterPropertiesSet()
	{
		sourceGeoEventDef  = getProperty("sourceGeoEventDef").getValueAsString();
		joinGeoEventDef = getProperty("joinGeoEventDef").getValueAsString();
		sourceKeyField = getProperty("sourceKeyField").getValueAsString();
		joinKeyField = getProperty("joinKeyField").getValueAsString();
		clearCacheOnStart = Converter.convertToBoolean(getProperty("clearCacheOnStart").getValueAsString());
	    geoEventDefinitionName = getProperty("newGeoEventDefinitionName").getValueAsString().trim();	
	}

	@Override
	public void setId(String id)
	{
		super.setId(id);
		geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
	}

	@Override
	public GeoEvent process(GeoEvent geoEvent) throws Exception
	{
		GeoEventDefinition ged = geoEvent.getGeoEventDefinition();
		String gedName = ged.getName();
		//if it came from the join put it in the cache
		if (gedName.equals(joinGeoEventDef))
		{
			String joinKeyFieldname = (String) geoEvent.getField(new FieldExpression(joinKeyField)).getValue();

			// Need to synchronize the Concurrent Map on write to avoid wrong counting
			synchronized (lock1)
			{
				// Add or update the cache
				joinTrackCache.put(joinKeyFieldname, geoEvent);
			}
		}
		else if (gedName.equals(sourceGeoEventDef)) //maybe we should allow any sources
		{
			String srcKeyFieldname = (String) geoEvent.getField(new FieldExpression(sourceKeyField)).getValue();
			GeoEvent joinGeoEvent = joinTrackCache.get(srcKeyFieldname);
			if (joinGeoEvent != null)
			{
				GeoEventDefinition edOut = lookup(geoEvent.getGeoEventDefinition(), joinGeoEvent.getGeoEventDefinition());
				Object[] objs = joinGeoEvent.getAllFields();
				GeoEvent geoEventOut = geoEventCreator.create(edOut.getGuid(), new Object[] { geoEvent.getAllFields(), joinGeoEvent.getAllFields() });
				geoEventOut.setProperty(GeoEventPropertyName.TYPE, "message");
				geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
				geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
				for (Map.Entry<GeoEventPropertyName, Object> property : geoEvent.getProperties())
					if (!geoEventOut.hasProperty(property.getKey()))
						geoEventOut.setProperty(property.getKey(), property.getValue());
				return geoEventOut;
			}
		}

		return null;
	}

	  synchronized private GeoEventDefinition lookup(GeoEventDefinition edIn, GeoEventDefinition joinGeoEventDefinition) throws Exception
	  {
	    GeoEventDefinition edOut = edMapper.containsKey(edIn.getGuid()) ? geoEventDefinitionManager.getGeoEventDefinition(edMapper.get(edIn.getGuid())) : null;
	    if (edOut == null)
	    {
	      List<FieldDefinition> joinFds = joinGeoEventDefinition.getFieldDefinitions();
	      if (joinFds != null)
	      {
	        edOut = edIn.augment(joinFds);        
	      }
	      edOut.setName(geoEventDefinitionName);
	      edOut.setOwner(getId());
	      geoEventDefinitionManager.addTemporaryGeoEventDefinition(edOut, geoEventDefinitionName.isEmpty());
	      edMapper.put(edIn.getGuid(), edOut.getGuid());
	    }
	    return edOut;
	  }
	
	  private void createGeoEventAndSend(GeoEvent sourceGeoEvent) throws MessagingException
	  {
	    //GeoEvent geoEventOut =  geoEventCreator.create(edOut.getGuid(), sourceGeoEvent.getAllFields());
	    GeoEvent geoEventOut = (GeoEvent) sourceGeoEvent.clone(null);
	    geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
	    geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
	    geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
	    for (Map.Entry<GeoEventPropertyName, Object> property : sourceGeoEvent.getProperties())
	    {
	      if (!geoEventOut.hasProperty(property.getKey()))
	      {
	        geoEventOut.setProperty(property.getKey(), property.getValue());                
	      }
	    }
	    send(geoEventOut);
	  }
	
	@Override
	public List<EventDestination> getEventDestinations()
	{
		return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
	}

	@Override
	public void validate() throws ValidationException
	{
		super.validate();
		List<String> errors = new ArrayList<String>();
		if (errors.size() > 0)
		{
			StringBuffer sb = new StringBuffer();
			for (String message : errors)
				sb.append(message).append("\n");
			throw new ValidationException(LOGGER.translate("VALIDATION_ERROR", this.getClass().getName(), sb.toString()));
		}
	}

	@Override
	public void onServiceStart()
	{
		if (this.clearCacheOnStart == true)
		{
			joinTrackCache.clear();
		}

		if (definition != null)
		{
			definitionUri = definition.getUri();
			definitionUriString = definitionUri.toString();
		}
	}

	@Override
	public void onServiceStop()
	{
		if (clearCacheTimer != null)
		{
			clearCacheTimer.cancel();
		}
		isIterating = false;
	}

	@Override
	public void shutdown()
	{
		super.shutdown();

		if (clearCacheTimer != null)
		{
			clearCacheTimer.cancel();
		}
	}

	@Override
	public EventDestination getEventDestination()
	{
		return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
	}

	@Override
	public void send(GeoEvent geoEvent) throws MessagingException
	{
		if (geoEventProducer != null && geoEvent != null)
		{
			geoEventProducer.send(geoEvent);
		}
	}

	public void setMessaging(Messaging messaging)
	{
		this.messaging = messaging;
		geoEventCreator = messaging.createGeoEventCreator();
	}

	@Override
	public void disconnect()
	{
		if (geoEventProducer != null)
			geoEventProducer.disconnect();
	}

	@Override
	public String getStatusDetails()
	{
		return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
	}

	@Override
	public void init() throws MessagingException
	{
		afterPropertiesSet();
	}

	@Override
	public boolean isConnected()
	{
		return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
	}

	  @Override
	  public void setup() throws MessagingException
	  {
	    ;
	  }

	  @Override
	  public void update(Observable o, Object arg)
	  {
	    ;
	  }

	  synchronized private void clearGeoEventDefinitionMapper()
	  {
	    if (!edMapper.isEmpty())
	    {
	      for (String guid : edMapper.values())
	      {
	        try
	        {
	          geoEventDefinitionManager.deleteGeoEventDefinition(guid);
	        }
	        catch (GeoEventDefinitionManagerException e)
	        {
	          ;
	        }
	      }
	      edMapper.clear();
	    }
	  }
	  
	  @Override
	  public Object addingService(ServiceReference reference)
	  {
	    Object service = definition.getBundleContext().getService(reference);
	    if (service instanceof GeoEventDefinitionManager)
	      this.geoEventDefinitionManager = (GeoEventDefinitionManager) service;
	    return service;
	  }

	  @Override
	  public void modifiedService(ServiceReference reference, Object service)
	  {
	    ;
	  }

	  @Override
	  public void removedService(ServiceReference reference, Object service)
	  {
	    if (service instanceof GeoEventDefinitionManager)
	    {
	      clearGeoEventDefinitionMapper();
	      this.geoEventDefinitionManager = null;
	    }
	  }
}
