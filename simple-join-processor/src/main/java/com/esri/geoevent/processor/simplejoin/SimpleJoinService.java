package com.esri.geoevent.processor.simplejoin;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class SimpleJoinService extends GeoEventProcessorServiceBase
{
  private Messaging messaging;

  public SimpleJoinService()
  {
    definition = new SimpleJoinDefinition();
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    SimpleJoin detector = new SimpleJoin(definition);
    detector.setMessaging(messaging);
    return detector;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }
}