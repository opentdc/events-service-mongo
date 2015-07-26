/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Arbalo AG
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.opentdc.events.mongo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.ServletContext;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.opentdc.mongo.AbstractMongodbServiceProvider;
import org.opentdc.events.EventModel;
import org.opentdc.events.InvitationState;
import org.opentdc.events.SalutationType;
import org.opentdc.events.ServiceProvider;
import org.opentdc.service.exception.DuplicateException;
import org.opentdc.service.exception.InternalServerErrorException;
import org.opentdc.service.exception.NotFoundException;
import org.opentdc.service.exception.ValidationException;
import org.opentdc.util.PrettyPrinter;

/**
 * A MongoDB-based implementation of the Events service.
 * @author Bruno Kaiser
 *
 */
public class MongodbServiceProvider 
	extends AbstractMongodbServiceProvider<EventModel> 
	implements ServiceProvider 
{
	
	private static final Logger logger = Logger.getLogger(MongodbServiceProvider.class.getName());

	/**
	 * Constructor.
	 * @param context the servlet context.
	 * @param prefix the simple class name of the service provider; this is also used as the collection name.
	 */
	public MongodbServiceProvider(
		ServletContext context, 
		String prefix) 
	{
		super(context);
		connect();
		collectionName = prefix;
		getCollection(collectionName);
		logger.info("MongodbServiceProvider(context, " + prefix + ") -> OK");
	}
	
	private Document convert(EventModel eventModel, boolean withId) 
	{
		Document _doc = new Document("firstName", eventModel.getFirstName())
			.append("lastName", eventModel.getLastName())
			.append("email", eventModel.getEmail())
			.append("comment", eventModel.getComment())
			.append("salutation", eventModel.getSalutation().toString())
			.append("invitationState", eventModel.getInvitationState().toString())
			.append("createdAt", eventModel.getCreatedAt())
			.append("createdBy", eventModel.getCreatedBy())
			.append("modifiedAt", eventModel.getModifiedAt())
			.append("modifiedBy", eventModel.getModifiedBy());
		if (withId == true) {
			_doc.append("_id", new ObjectId(eventModel.getId()));
		}
		return _doc;
	}
	
	private EventModel convert(Document doc)
	{
		EventModel _model = new EventModel();
		_model.setId(doc.getObjectId("_id").toString());
		_model.setFirstName(doc.getString("firstName"));
		_model.setLastName(doc.getString("lastName"));
		_model.setEmail(doc.getString("email"));
		_model.setComment(doc.getString("comment"));
		_model.setSalutation(SalutationType.valueOf(doc.getString("salutation")));
		_model.setInvitationState(InvitationState.valueOf(doc.getString("invitationState")));
		_model.setCreatedAt(doc.getDate("createdAt"));
		_model.setCreatedBy(doc.getString("createdBy"));
		_model.setModifiedAt(doc.getDate("modifiedAt"));
		_model.setModifiedBy(doc.getString("modifiedBy"));
		return _model;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#list(java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public ArrayList<EventModel> list(
		String queryType,
		String query,
		int position,
		int size) {
		List<Document> _docs = list(position, size);
		ArrayList<EventModel> _selection = new ArrayList<EventModel>();
		for (Document doc : _docs) {
			_selection.add(convert(doc));
		}
		logger.info("list(<" + query + ">, <" + queryType + 
				">, <" + position + ">, <" + size + ">) -> " + _selection.size() + " events.");
		return _selection;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#create(org.opentdc.events.EventsModel)
	 */
	@Override
	public EventModel create(
		EventModel event) 
	throws DuplicateException, ValidationException {
		logger.info("create(" + PrettyPrinter.prettyPrintAsJSON(event) + ")");
		if (event.getId() != null && !event.getId().isEmpty()) {
			throw new ValidationException("event <" + event.getId() + "> contains an id generated on the client.");
		}
		// enforce mandatory fields
		if (event.getFirstName() == null || event.getFirstName().length() == 0) {
			throw new ValidationException("event must contain a valid firstName.");
		}
		if (event.getLastName() == null || event.getLastName().length() == 0) {
			throw new ValidationException("event must contain a valid lastName.");
		}
		if (event.getEmail() == null || event.getEmail().length() == 0) {
			throw new ValidationException("event must contain a valid email address.");
		}
		// set default values
		if (event.getInvitationState() == null) {
			event.setInvitationState(InvitationState.INITIAL);
		}
		if (event.getSalutation() == null) {
			event.setSalutation(SalutationType.DU_M);
		}
		// set modification / creation values
		Date _date = new Date();
		event.setCreatedAt(_date);
		event.setCreatedBy(getPrincipal());
		event.setModifiedAt(_date);
		event.setModifiedBy(getPrincipal());
		
		create(convert(event, false));
		logger.info("create(" + PrettyPrinter.prettyPrintAsJSON(event) + ")");
		return event;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#read(java.lang.String)
	 */
	@Override
	public EventModel read(
		String id) 
	throws NotFoundException {
		EventModel _event = convert(readOne(id));
		if (_event == null) {
			throw new NotFoundException("no event with ID <" + id
					+ "> was found.");
		}
		logger.info("read(" + id + ") -> " + PrettyPrinter.prettyPrintAsJSON(_event));
		return _event;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#update(java.lang.String, org.opentdc.events.EventsModel)
	 */
	@Override
	public EventModel update(
		String id, 
		EventModel event
	) throws NotFoundException, ValidationException {
		EventModel _event = read(id);
		if (! _event.getCreatedAt().equals(event.getCreatedAt())) {
			logger.warning("event <" + id + ">: ignoring createdAt value <" + event.getCreatedAt().toString() + 
					"> because it was set on the client.");
		}
		if (! _event.getCreatedBy().equalsIgnoreCase(event.getCreatedBy())) {
			logger.warning("event <" + id + ">: ignoring createdBy value <" + event.getCreatedBy() +
					"> because it was set on the client.");
		}
		if (event.getFirstName() == null || event.getFirstName().length() == 0) {
			throw new ValidationException("event <" + id + 
					"> must contain a valid firstName.");
		}
		if (event.getLastName() == null || event.getLastName().length() == 0) {
			throw new ValidationException("event <" + id + 
					"> must contain a valid lastName.");
		}
		if (event.getEmail() == null || event.getEmail().length() == 0) {
			throw new ValidationException("event <" + id + 
					"> must contain a valid email address.");
		}
		if (event.getInvitationState() == null) {
			event.setInvitationState(InvitationState.INITIAL);
		}
		if (event.getSalutation() == null) {
			event.setSalutation(SalutationType.DU_M);
		}
		_event.setFirstName(event.getFirstName());
		_event.setLastName(event.getLastName());
		_event.setEmail(event.getEmail());
		_event.setSalutation(event.getSalutation());
		_event.setInvitationState(event.getInvitationState());
		_event.setComment(event.getComment());
		_event.setModifiedAt(new Date());
		_event.setModifiedBy(getPrincipal());
		update(id, convert(_event, true));
		logger.info("update(" + id + ") -> " + PrettyPrinter.prettyPrintAsJSON(_event));
		return _event;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#delete(java.lang.String)
	 */
	@Override
	public void delete(
		String id) 
	throws NotFoundException, InternalServerErrorException {
		read(id);
		deleteOne(id);
		logger.info("delete(" + id + ") -> OK");
	}
}
