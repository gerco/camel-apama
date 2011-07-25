package example;

import nl.gdries.camel.component.apama.ApamaEventRegistrar;

import com.apama.event.parser.DictionaryFieldType;
import com.apama.event.parser.EventType;
import com.apama.event.parser.Field;
import com.apama.event.parser.IntegerFieldType;
import com.apama.event.parser.SequenceFieldType;
import com.apama.event.parser.StringFieldType;

public class EventRegistrar implements ApamaEventRegistrar {

	private static EventType[] eventTypes = new EventType[] {
			/**
			 * The corresponding MonitorScript event definition is:
			 * 
			 * package example;
			 * 
			 * event Event {
			 *   integer intValue;
			 *   sequence<integer> intSeq;
			 *   dictionary<integer, string> dict;
			 * }
			 */
			new EventType("example.Event", new Field[] {
				new Field("intValue", IntegerFieldType.TYPE),
				new Field("intSeq", new SequenceFieldType(IntegerFieldType.TYPE)),
				new Field("dict", new DictionaryFieldType(
						IntegerFieldType.TYPE, StringFieldType.TYPE))
			})
		};
	
	@Override
	public EventType[] getEventTypes() {
		return eventTypes;
	}

}
