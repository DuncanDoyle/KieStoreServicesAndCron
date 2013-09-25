package org.jboss.ddoyle.brms.cep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.core.ClockType;
import org.drools.core.RuleBaseConfiguration;
import org.drools.core.time.SessionPseudoClock;
import org.drools.persistence.info.SessionInfo;
import org.drools.persistence.info.WorkItemInfo;
import org.drools.persistence.map.EnvironmentBuilder;
import org.drools.persistence.map.KnowledgeSessionStorage;
import org.drools.persistence.map.KnowledgeSessionStorageEnvironmentBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.persistence.jpa.KieStoreServices;
import org.kie.api.runtime.Environment;
import org.kie.api.runtime.EnvironmentName;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;
import org.kie.internal.KnowledgeBaseFactory;


public class TestStorage {

	private SimpleKnowledgeSessionStorage storage;

	@Before
	public void initStorage() {
		storage = new SimpleKnowledgeSessionStorage();
	}

	@Test
	public void testStorageWithTimerAndPseudoClock() throws InterruptedException {
		KieSession session = getSession(null, ClockType.PSEUDO_CLOCK);
		SessionPseudoClock sessionClock = session.getSessionClock();
		int id = session.getId();
		
		//Fire the rules.
		session.fireAllRules();
		
		//Thread.sleep(2000);
		sessionClock.advanceTime(2, TimeUnit.SECONDS);
		session.fireAllRules();

		final int numberOfFacts = session.getFactHandles().size();
		System.out.println("Number of facts should be 2: " + numberOfFacts);
		
		Assert.assertTrue(numberOfFacts >= 1);
		session.dispose();

		//Thread.sleep(3000);
		session = getSession(id, ClockType.PSEUDO_CLOCK);
		SessionPseudoClock loadedSessionClock = session.getSessionClock();
		loadedSessionClock.advanceTime(3, TimeUnit.SECONDS);
		session.fireAllRules();
		
		int afterLoadNumberOfFacts = session.getFactHandles().size();
		assertEquals(3,  afterLoadNumberOfFacts);
		assertTrue(afterLoadNumberOfFacts > numberOfFacts);
		
		loadedSessionClock.advanceTime(3,  TimeUnit.SECONDS);
		session.fireAllRules();
		
		afterLoadNumberOfFacts = session.getFactHandles().size();
		assertEquals(4,  afterLoadNumberOfFacts);
		assertTrue(afterLoadNumberOfFacts > numberOfFacts);
		
		session.dispose();
	}
	
	@Test
	public void testStorageWithTimerAndRealtimeClock() throws InterruptedException {
		KieSession session = getSession(null, ClockType.REALTIME_CLOCK);
		int id = session.getId();
		
		//Fire the rules.
		session.fireAllRules();
		
		Thread.sleep(2000);
		//sessionClock.advanceTime(2, TimeUnit.SECONDS);
		session.fireAllRules();

		final int numberOfFacts = session.getFactHandles().size();
		System.out.println("Number of facts should be 2: " + numberOfFacts);
		
		Assert.assertTrue(numberOfFacts >= 1);
		session.dispose();

		Thread.sleep(3000);
		session = getSession(id, ClockType.REALTIME_CLOCK);
		//SessionPseudoClock loadedSessionClock = session.getSessionClock();
		//loadedSessionClock.advanceTime(3, TimeUnit.SECONDS);
		session.fireAllRules();
		
		int afterLoadNumberOfFacts = session.getFactHandles().size();
		System.out.println("Number of facts, probably 3: " + afterLoadNumberOfFacts);
		Assert.assertTrue(afterLoadNumberOfFacts > numberOfFacts);
		
		Thread.sleep(3000);
		//loadedSessionClock.advanceTime(3,  TimeUnit.SECONDS);
		session.fireAllRules();
		
		afterLoadNumberOfFacts = session.getFactHandles().size();
		System.out.println("Number of facts, probably 4: " + afterLoadNumberOfFacts);
		Assert.assertTrue(afterLoadNumberOfFacts > numberOfFacts);
		
		session.dispose();
	}
	
	private String getRule() {
		String rule = "package test\n";
		rule += "import erdf.poc.cep.*;\n";
		rule += "rule \"test\"\n";
		//Fire the rule every seconds ...
		rule += "timer (cron:0/2 * * * * ?)\n";
		rule += "when\n";
		rule += "then\n";
		rule += "insert(\"test\"+System.currentTimeMillis());\n";
		rule += "System.out.println(\"Firing rule!!!\");\n";
		rule += "end";
        return rule;
	}
	
	private KieSession getSession(Integer i, ClockType clockType) {  	

		final Environment env = KnowledgeBaseFactory.newEnvironment();
		EnvironmentBuilder envBuilder = new KnowledgeSessionStorageEnvironmentBuilder(storage);
		env.set(EnvironmentName.TRANSACTION_MANAGER, envBuilder.getTransactionManager());
		env.set(EnvironmentName.PERSISTENCE_CONTEXT_MANAGER, envBuilder.getPersistenceContextManager());
		KieStoreServices storeS = KieServices.Factory.get().getStoreServices();

		KieServices ks = KieServices.Factory.get();
		KieFileSystem kfs = ks.newKieFileSystem();
      	kfs.write("src/main/resources/test.drl", getRule());
      	KieBuilder kbuilder = ks.newKieBuilder(kfs).buildAll();
		Iterator<Message> iter = kbuilder.getResults().getMessages().iterator();
		while (iter.hasNext()) {
			System.out.println("Error: "+iter.next());
		}

		KieContainer kieContainer = ks.newKieContainer(kbuilder.getKieModule().getReleaseId()); 
		//KieBaseConfiguration kieBaseConfiguration = ks.newKieBaseConfiguration();
		//final KieBase kieBase = kieContainer.newKieBase(kieBaseConfiguration);
		final KieBase kieBase = kieContainer.newKieBase(new RuleBaseConfiguration());

		KieSessionConfiguration conf = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
		//conf.setOption( TimerJobFactoryOption.get("trackable"));
        //Use the PseudoClock so we have a bit more control in our tests.
        conf.setOption(ClockTypeOption.get(clockType.getId()));

		KieSession session;
		if (i != null) {
			System.out.println("Loading KieSession");
			session = storeS.loadKieSession(i, kieBase, conf, env);
		} else {
			System.out.println("Creating new KieSession");
        	session = storeS.newKieSession(kieBase, conf, env);
		}

		return session;
		
    }
	
	private static class SimpleKnowledgeSessionStorage implements KnowledgeSessionStorage {

		public Map<Integer, SessionInfo> ksessions = new HashMap<Integer, SessionInfo>();
		public Map<Long, WorkItemInfo> workItems = new HashMap<Long, WorkItemInfo>();

		public SessionInfo findSessionInfo(Integer id) {
			return ksessions.get(id);
		}

		public void saveOrUpdate(SessionInfo storedObject) {
			ksessions.put(storedObject.getId(), storedObject);
		}

		public void saveOrUpdate(WorkItemInfo workItemInfo) {
			workItems.put(workItemInfo.getId(), workItemInfo);
		}

		public Long getNextWorkItemId() {
			return new Long(workItems.size() + 1);
		}

		public WorkItemInfo findWorkItemInfo(Long id) {
			return workItems.get(id);
		}

		public void remove(WorkItemInfo workItemInfo) {
			workItems.remove(workItemInfo.getId());
		}

		public Integer getNextStatefulKnowledgeSessionId() {
			return ksessions.size() + 1;
		}

		@Override
		public void lock(SessionInfo arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void lock(WorkItemInfo arg0) {
			// TODO Auto-generated method stub
			
		}
	}
}
