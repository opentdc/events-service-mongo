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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.opentdc.util.EmailSender;
import org.opentdc.util.FreeMarkerConfig;
import org.opentdc.util.PrettyPrinter;

import freemarker.template.Template;

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
	private EmailSender emailSender = null;
	private static final String SUBJECT = "Einladung zum Arbalo Launch Event";

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
		new FreeMarkerConfig(context);
		emailSender = new EmailSender(context);
		logger.info("MongodbServiceProvider(context, " + prefix + ") -> OK");
	}
	
	private Document convert(EventModel eventModel, boolean withId) 
	{
		Document _doc = new Document("firstName", eventModel.getFirstName())
			.append("lastName", eventModel.getLastName())
			.append("email", eventModel.getEmail())
			.append("comment", eventModel.getComment())
			.append("internalComment", eventModel.getInternalComment())
			.append("contact",  eventModel.getContact())
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
		_model.setComment(doc.getString("internalComment"));
		_model.setContact(doc.getString("contact"));
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
		event.setId(new ObjectId().toString());
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
		
		create(convert(event, true));
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
		_event.setContact(event.getContact());
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

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#getMessage(java.lang.String)
	 */
	@Override
	public String getMessage(String id) throws NotFoundException,
			InternalServerErrorException {
		logger.info("getMessage(" + id + ")");
		EventModel _model = read(id);
		
		// create the FreeMarker data model
        Map<String, Object> _root = new HashMap<String, Object>();    
        _root.put("event", _model);
        
        // Merge data model with template   
        String _msg = FreeMarkerConfig.getProcessedTemplate(
        		_root, 
        		getTemplate(_model.getSalutation(), _model.getContact()));
		logger.info("getMessage(" + id + ") -> " + _msg);
		return _msg;
	}
	
	/**
	 * Retrieve the email address of the contact.
	 * @param userName the name of the contact
	 * @return the corresponding email address
	 */
	private String getEmailAddress(String userName) {
		logger.info("getEmailAddress(" + userName + ")");
		String _emailAddress = null;
		if (userName == null || userName.isEmpty()) {
			userName = "arbalo";
		}
	       if (userName.equalsIgnoreCase("bruno")) {
	        	_emailAddress = "bruno.kaiser@arbalo.ch";
	        } else if (userName.equalsIgnoreCase("thomas")) {
	        	_emailAddress = "thomas.huber@arbalo.ch";
	        } else if (userName.equalsIgnoreCase("peter")) {
	        	_emailAddress = "peter.windemann@arbalo.ch";
	        } else if (userName.equalsIgnoreCase("marc")) {
	        	_emailAddress = "marc.hofer@arbalo.ch";
	        } else if (userName.equalsIgnoreCase("werner")) {
	        	_emailAddress = "werner.froidevaux@arbalo.ch";        	
	        } else {
	        	_emailAddress = "info@arbalo.ch";        	        	
	        }
	        logger.info("getEmailAddress(" + userName + ") -> " + _emailAddress);
	        return _emailAddress;	
	}
	
	/**
	 * @param salutation
	 * @return
	 */
	private Template getTemplate(
			SalutationType salutation, String userName) {
		String _templateName = null;
		if (userName == null || userName.isEmpty()) {
			userName = "arbalo";
		}
		switch (salutation) {
		case HERR: _templateName = "emailHerr_" + userName + ".ftl"; break;
		case FRAU: _templateName = "emailFrau_" + userName + ".ftl"; break;
		case DU_F: _templateName = "emailDuf_" + userName + ".ftl";  break;
		case DU_M: _templateName = "emailDum_" + userName + ".ftl";  break;
		}
		return FreeMarkerConfig.getTemplateByName(_templateName);
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#sendMessage(java.lang.String)
	 */
	@Override
	public void sendMessage(
			String id) 
			throws NotFoundException, InternalServerErrorException {
		logger.info("sendMessage(" + id + ")");
		EventModel _model = read(id);

		emailSender.sendMessage(
			_model.getEmail(),
			getEmailAddress(_model.getContact()),
			SUBJECT,
			getMessage(id));
		logger.info("sent email message to " + _model.getEmail());
		_model.setId(null);
		_model.setInvitationState(InvitationState.SENT);
		update(id, _model);
	}

	/* (non-Javadoc)
	 * @see org.opentdc.events.ServiceProvider#sendAllMessages()
	 */
	@Override
	public void sendAllMessages() 
			throws InternalServerErrorException {
		logger.info("sendAllMessages()");
		EventModel _model = null;
		String _id = null;
		for (Document doc : list(0, 200)) {
			_model = convert(doc);
			_id = _model.getId();
			emailSender.sendMessage(
				_model.getEmail(),
				getEmailAddress(_model.getContact()),
				SUBJECT,
				getMessage(_id));
			logger.info("sent email message to " + _model.getEmail());
			_model.setId(null);
			_model.setInvitationState(InvitationState.SENT);
			update(_id, _model);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException _ex) {
				_ex.printStackTrace();
				throw new InternalServerErrorException(_ex.getMessage());
			}
		}
	}
}
