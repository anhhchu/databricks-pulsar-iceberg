#!/usr/bin/env python3
"""
Pulsar Financial Message Producer
Generates and sends financial instrument analysis messages to Apache Pulsar

Local Setup (Current Configuration):
1. Install Pulsar: brew install apache-pulsar
2. Start Pulsar: pulsar standalone
3. Run this script: python pulsar_financial_message_producer.py
4. View messages at: http://localhost:8080

AWS Setup (see commented configuration in main()):
- Requires EC2 instance with Pulsar
- Authentication configuration needed
- Update service URL to your EC2 instance
"""

import sys
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pulsar
from dataclasses import dataclass, asdict
import logging
import requests

# Add parent directory to path to import shared config
sys.path.append('..')
from config import DEV_CONFIG, PROD_CONFIG, PRODUCER_CONFIG, DEFAULT_INSTRUMENT_CONFIG, DEFAULT_RISK_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class InstrumentReference:
    """Financial instrument reference data"""
    analysisidentifier: str
    instrumentidentifier: str
    asofdate: str
    accountidentifier: str
    accountname: Optional[str]
    instrumentname: Optional[str]
    description: str
    instrumenttype: str
    instrumentsubtype: Optional[str]
    consumerproductcategory: Optional[str]
    originationdate: str
    maturitydate: str
    amortizationtype: str
    amortizationenddate: str
    isinterestonly: Optional[bool]
    cashflowtype: Optional[str]
    instrumentcurrency: str
    notionalportion: Optional[float]
    unpaidprincipalbalance: str
    currentcommitmentamount: Optional[float]
    marketpriceoverride: str
    fixedpaymentamount: Optional[float]
    currentbookpriceoverride: str
    interestratetype: str
    interestpaymentfrequency: str
    curerate: Optional[float]
    fixedrate: Optional[float]
    currentrate: float
    portfolioidentifier: str
    interestratespread: float
    interestrateindexmultiplier: float
    interestrateindex: str
    lifetimeinterestratecap: float
    lifetimeinterestratefloor: float
    periodicinterestratecap: float
    periodicratefloor: float
    interestrateresetfirstdate: Optional[str]
    interestrateresetfrequency: Optional[str]
    daycount: str
    optionadjustedspreadoverride: float
    modified: Optional[str]
    parmarketprice: Optional[float]
    servicingspread: float
    company: str
    discountcurve: str
    accountside: str
    jobidentifier: str
    cashfloworder: int
    cashflowsource: str
    cashflowmodelname: str
    prepaymentorder: int
    prepaymentsource: str
    prepaymentmodelname: str
    prepaymentshift: float
    prepaymentscalingfactor: float

@dataclass
class InstrumentRiskMetric:
    """Risk metrics for financial instruments"""
    analysisidentifier: Optional[str]
    reportingdate: Optional[str]
    inputscenarioidentifier: Optional[str]
    instrumentidentifier: str
    scenarioidentifier: str
    modelname: str
    modeloutput: str
    asofdate: str
    term: float
    timesegment: Optional[str]
    annualizedcumulativepd: Optional[float]
    forwardpd: Optional[float]
    cumulativepd: Optional[float]
    marginalpd: Optional[float]
    maturityriskpd: Optional[float]
    maturityriskel: Optional[float]
    lgd: Optional[float]
    maturityrisklgd: Optional[float]
    lossrateannualized: Optional[float]
    lossratecumulative: Optional[float]
    ead: Optional[float]
    ccf: Optional[float]
    ugd: Optional[float]
    prepaymentrate: Optional[float]
    forwardprepaymentrate: Optional[float]
    cumulativeprepaymentrate: Optional[float]
    recovery: Optional[float]
    netchargeoff: Optional[float]
    annualizedpdoneyearprojection: Optional[float]
    stage1conditionalannualizedcumulativepd: Optional[float]
    stage2conditionalannualizedcumulativepd: Optional[float]
    stage3conditionalannualizedcumulativepd: Optional[float]
    impliedstagerating: Optional[str]
    netchargeoffamount: Optional[float]
    collateralvalue: Optional[float]
    expectedcreditlossamount: Optional[float]
    expectedcreditlossamountlifetimeprojection: Optional[float]
    expectedcreditlossamountoneyearprojection: Optional[float]
    exposure: Optional[float]
    grossinterestincome: Optional[float]
    totalinterestexpense: Optional[float]
    riskweightedassets: Optional[float]
    stage1portion: Optional[float]
    stage2portion: Optional[float]
    stage3portion: Optional[float]
    transitionprobabilityfromstage1tostage2: Optional[float]
    transitionprobabilityfromstage1tostage3: Optional[float]
    transitionprobabilityfromstage2tostage1: Optional[float]
    transitionprobabilityfromstage2tostage3: Optional[float]
    transitionprobabilityfromstage3tostage2: Optional[float]
    balancegrowthrate: Optional[float]
    lgdvariance: Optional[float]
    transactionsequence: Optional[int]
    creditotherthantemporaryimpairment: Optional[float]
    noncreditotherthantemporaryimpairment: Optional[float]
    temporaryimpairment: Optional[float]
    othercomprehensiveincome: Optional[float]
    otherthantemporaryimpairmentprobability: Optional[float]
    jobidentifier: Optional[str]
    valuedate: Optional[str]
    decayrate: Optional[float]
    rateresponserate: Optional[float]
    usagerate: Optional[float]
    liquidityhaircut: Optional[float]
    singlemonthlymortalityrate: Optional[float]
    edfimpliedrating: Optional[str]
    optionarmminimumpaymentportion: Optional[float]
    optionarminterestonlyportion: Optional[float]
    optionarmprincipalandinterestportion: Optional[float]
    forbearanceportion: Optional[float]
    forwarddecayrate: Optional[float]

@dataclass
class InstrumentError:
    """Error information for instruments"""
    analysisidentifier: str
    jobidentifier: str
    instrumentidentifier: str
    errorcode: int
    errormessage: str
    modulecode: int
    asofdate: Optional[str]
    scenarioidentifier: Optional[str]
    severity: str
    portfolioidentifier: str

@dataclass
class InstrumentData:
    """Complete instrument data structure"""
    type: str
    instrumentreference: InstrumentReference
    instrumentriskmetric: List[InstrumentRiskMetric]
    instrumentcashflow: Optional[Any]
    instrumenttimebucketmeasures: Optional[Any]
    instrumenterror: List[InstrumentError]
    accounttimebucketmeasures: Optional[Any]
    accountcashflow: Optional[Any]

@dataclass
class FinancialMessage:
    """Complete financial analysis message"""
    jobidentifier: str
    analysisidentifier: str
    data: List[InstrumentData]

class PulsarFinancialMessageProducer:
    """Pulsar producer for financial messages with configurable setup"""
    
    def __init__(self, service_url: str, topic: str, auth_params: Optional[Dict] = None, producer_config: Optional[Dict] = None):
        """
        Initialize Pulsar producer
        
        Args:
            service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650' or 'pulsar://your-ec2-ip:6650')
            topic: Topic name to publish messages to
            auth_params: Authentication parameters (optional for local setup)
            producer_config: Producer configuration settings
        """
        self.service_url = service_url
        self.topic = topic
        self.client = None
        self.producer = None
        self.auth_params = auth_params or {}
        self.producer_config = producer_config or PRODUCER_CONFIG
        
    def connect(self):
        """Establish connection to Pulsar"""
        try:
            # Configure authentication if provided
            auth = None
            if self.auth_params:
                # Example for OAuth2 authentication with AWS
                if 'oauth2' in self.auth_params:
                    auth = pulsar.AuthenticationOauth2(**self.auth_params['oauth2'])
                elif 'token' in self.auth_params:
                    auth = pulsar.AuthenticationToken(self.auth_params['token'])
            
            self.client = pulsar.Client(
                service_url=self.service_url,
                authentication=auth,
                operation_timeout_seconds=30
            )
            
            # Use configuration for producer settings with safe compression type handling
            compression_type = pulsar.CompressionType.LZ4  # Default to LZ4
            
            # Safely handle compression type configuration
            requested_compression = self.producer_config.get('compression_type', 'LZ4').upper()
            
            try:
                if requested_compression == 'NONE':
                    compression_type = pulsar.CompressionType.NONE
                elif requested_compression == 'LZ4':
                    compression_type = pulsar.CompressionType.LZ4
                elif requested_compression == 'ZLIB' and hasattr(pulsar.CompressionType, 'ZLIB'):
                    compression_type = pulsar.CompressionType.ZLIB
                elif requested_compression == 'ZSTD' and hasattr(pulsar.CompressionType, 'ZSTD'):
                    compression_type = pulsar.CompressionType.ZSTD
                else:
                    logger.warning(f"Compression type '{requested_compression}' not available, using LZ4")
                    compression_type = pulsar.CompressionType.LZ4
            except AttributeError:
                logger.warning(f"Compression type '{requested_compression}' not supported in this Pulsar version, using LZ4")
                compression_type = pulsar.CompressionType.LZ4
            
            self.producer = self.client.create_producer(
                topic=self.topic,
                compression_type=compression_type,
                batching_enabled=self.producer_config.get('batching_enabled', True),
                batching_max_publish_delay_ms=self.producer_config.get('batch_max_delay_ms', 100),
                batching_max_messages=self.producer_config.get('batch_max_messages', 100),
                send_timeout_millis=self.producer_config.get('send_timeout_ms', 30000),
                max_pending_messages=self.producer_config.get('max_pending_messages', 1000)
            )
            
            logger.info(f"Connected to Pulsar at {self.service_url}, topic: {self.topic}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            raise
    
    def disconnect(self):
        """Close Pulsar connections"""
        if self.producer:
            self.producer.close()
        if self.client:
            self.client.close()
        logger.info("Disconnected from Pulsar")
    
    def create_sample_instrument_reference(self, analysis_id: str, job_id: str) -> InstrumentReference:
        """Create a sample instrument reference using configuration defaults"""
        return InstrumentReference(
            analysisidentifier=analysis_id,
            instrumentidentifier=f"Bond_{uuid.uuid4().hex[:8]}",
            asofdate=datetime.now().strftime("%Y-%m-%d"),
            accountidentifier="TPS/CD/CP_AFS",
            accountname=None,
            instrumentname=None,
            description="Corporate Bond Investment",
            instrumenttype=DEFAULT_INSTRUMENT_CONFIG['instrument_type'],
            instrumentsubtype=None,
            consumerproductcategory=None,
            originationdate=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            maturitydate=(datetime.now() + timedelta(days=1825)).strftime("%Y-%m-%d"),  # 5 years
            amortizationtype="Constant installment",
            amortizationenddate=(datetime.now() + timedelta(days=1825)).strftime("%Y-%m-%d"),
            isinterestonly=None,
            cashflowtype=None,
            instrumentcurrency=DEFAULT_INSTRUMENT_CONFIG['currency'],
            notionalportion=None,
            unpaidprincipalbalance="1000000",
            currentcommitmentamount=None,
            marketpriceoverride="102.5",
            fixedpaymentamount=None,
            currentbookpriceoverride="100",
            interestratetype="Fixed",
            interestpaymentfrequency="Semi-Annual",
            curerate=None,
            fixedrate=None,
            currentrate=0.0325,  # 3.25%
            portfolioidentifier=f"{DEFAULT_INSTRUMENT_CONFIG['portfolio_prefix']}_01",
            interestratespread=0.0,
            interestrateindexmultiplier=0.0,
            interestrateindex="10YT",
            lifetimeinterestratecap=99.0,
            lifetimeinterestratefloor=0.0,
            periodicinterestratecap=99.0,
            periodicratefloor=0.0,
            interestrateresetfirstdate=None,
            interestrateresetfrequency=None,
            daycount=DEFAULT_INSTRUMENT_CONFIG['day_count'],
            optionadjustedspreadoverride=0.0,
            modified=None,
            parmarketprice=None,
            servicingspread=0.0,
            company=DEFAULT_INSTRUMENT_CONFIG['company'],
            discountcurve=DEFAULT_INSTRUMENT_CONFIG['discount_curve'],
            accountside=DEFAULT_INSTRUMENT_CONFIG['account_side'],
            jobidentifier=job_id,
            cashfloworder=10000,
            cashflowsource="API model",
            cashflowmodelname="Standard Cash Flow Model",
            prepaymentorder=10000,
            prepaymentsource="Statistical model",
            prepaymentmodelname="Standard Prepayment Model",
            prepaymentshift=0.0,
            prepaymentscalingfactor=1.0
        )
    
    def create_sample_risk_metrics(self, instrument_id: str, analysis_dates: List[str]) -> List[InstrumentRiskMetric]:
        """Create sample risk metrics for multiple dates using configuration defaults"""
        metrics = []
        for date in analysis_dates:
            metric = InstrumentRiskMetric(
                analysisidentifier=None,
                reportingdate=None,
                inputscenarioidentifier=None,
                instrumentidentifier=instrument_id,
                scenarioidentifier=DEFAULT_RISK_CONFIG['scenario_identifier'],
                modelname=DEFAULT_RISK_CONFIG['model_name'],
                modeloutput=DEFAULT_RISK_CONFIG['model_output'],
                asofdate=date,
                term=1.0,
                timesegment=None,
                annualizedcumulativepd=DEFAULT_RISK_CONFIG['default_pd'],
                forwardpd=0.011,
                cumulativepd=DEFAULT_RISK_CONFIG['default_pd'],
                marginalpd=0.001,
                maturityriskpd=None,
                maturityriskel=None,
                lgd=DEFAULT_RISK_CONFIG['default_lgd'],
                maturityrisklgd=None,
                lossrateannualized=0.0054,  # 0.54%
                lossratecumulative=0.0054,
                ead=1000000.0,  # Exposure at Default
                ccf=None,
                ugd=None,
                prepaymentrate=None,
                forwardprepaymentrate=0.15,  # 15% annual prepayment
                cumulativeprepaymentrate=None,
                recovery=1 - DEFAULT_RISK_CONFIG['default_lgd'],  # 55% recovery
                netchargeoff=None,
                annualizedpdoneyearprojection=0.013,
                stage1conditionalannualizedcumulativepd=0.01,
                stage2conditionalannualizedcumulativepd=0.05,
                stage3conditionalannualizedcumulativepd=0.95,
                impliedstagerating="Investment Grade",
                netchargeoffamount=None,
                collateralvalue=None,
                expectedcreditlossamount=5400.0,  # ECL amount
                expectedcreditlossamountlifetimeprojection=27000.0,
                expectedcreditlossamountoneyearprojection=5400.0,
                exposure=1000000.0,
                grossinterestincome=32500.0,  # Annual interest
                totalinterestexpense=None,
                riskweightedassets=1000000.0 * DEFAULT_RISK_CONFIG['risk_weight'],
                stage1portion=0.85,
                stage2portion=0.12,
                stage3portion=0.03,
                transitionprobabilityfromstage1tostage2=0.05,
                transitionprobabilityfromstage1tostage3=0.002,
                transitionprobabilityfromstage2tostage1=0.15,
                transitionprobabilityfromstage2tostage3=0.08,
                transitionprobabilityfromstage3tostage2=0.1,
                balancegrowthrate=0.02,
                lgdvariance=None,
                transactionsequence=None,
                creditotherthantemporaryimpairment=None,
                noncreditotherthantemporaryimpairment=None,
                temporaryimpairment=None,
                othercomprehensiveincome=None,
                otherthantemporaryimpairmentprobability=None,
                jobidentifier=None,
                valuedate=None,
                decayrate=None,
                rateresponserate=None,
                usagerate=None,
                liquidityhaircut=None,
                singlemonthlymortalityrate=None,
                edfimpliedrating="BBB",
                optionarmminimumpaymentportion=None,
                optionarminterestonlyportion=None,
                optionarmprincipalandinterestportion=None,
                forbearanceportion=None,
                forwarddecayrate=None
            )
            metrics.append(metric)
        return metrics
    
    def create_sample_errors(self, analysis_id: str, job_id: str, instrument_id: str) -> List[InstrumentError]:
        """Create sample instrument errors"""
        return [
            InstrumentError(
                analysisidentifier=analysis_id,
                jobidentifier=job_id,
                instrumentidentifier=instrument_id,
                errorcode=1,
                errormessage=f"Warning: High prepayment rate detected for instrument {instrument_id}",
                modulecode=101,
                asofdate=None,
                scenarioidentifier=None,
                severity="Warning",
                portfolioidentifier=f"{DEFAULT_INSTRUMENT_CONFIG['portfolio_prefix']}_01"
            )
        ]
    
    def generate_financial_message(self) -> FinancialMessage:
        """Generate a complete financial message"""
        job_id = str(uuid.uuid4())
        analysis_id = str(uuid.uuid4())
        
        # Create instrument reference
        instrument_ref = self.create_sample_instrument_reference(analysis_id, job_id)
        instrument_id = instrument_ref.instrumentidentifier
        
        # Create risk metrics for current and next year
        current_date = datetime.now().strftime("%Y-%m-%d")
        next_year_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        risk_metrics = self.create_sample_risk_metrics(instrument_id, [current_date, next_year_date])
        
        # Create errors
        errors = self.create_sample_errors(analysis_id, job_id, instrument_id)
        
        # Create instrument data
        instrument_data = InstrumentData(
            type="instrument",
            instrumentreference=instrument_ref,
            instrumentriskmetric=risk_metrics,
            instrumentcashflow=None,
            instrumenttimebucketmeasures=None,
            instrumenterror=errors,
            accounttimebucketmeasures=None,
            accountcashflow=None
        )
        
        # Create complete message
        message = FinancialMessage(
            jobidentifier=job_id,
            analysisidentifier=analysis_id,
            data=[instrument_data]
        )
        
        return message
    
    def send_message(self, message: FinancialMessage, message_key: Optional[str] = None) -> str:
        """Send financial message to Pulsar"""
        try:
            # Convert to dictionary and then to JSON
            message_dict = asdict(message)
            message_json = json.dumps(message_dict, indent=2)
            
            # Send message
            message_id = self.producer.send(
                content=message_json.encode('utf-8'),
                partition_key=message_key or message.jobidentifier,
                properties={
                    'message_type': 'financial_analysis',
                    'job_id': message.jobidentifier,
                    'analysis_id': message.analysisidentifier,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            logger.info(f"Message sent successfully. Message ID: {message_id}")
            return message_id
            
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise
    
    def send_sample_message(self) -> str:
        """Generate and send a sample financial message"""
        message = self.generate_financial_message()
        return self.send_message(message)

def main():
    """Example usage of the Pulsar Financial Message Producer using configuration"""
    
    # Use DEV_CONFIG for local development, PROD_CONFIG for production
    config = PROD_CONFIG  # Change to PROD_CONFIG for production use
    
    service_url = config['service_url']
    topic = config['topic']
    auth_config = config['auth']
    log_level = config['log_level']
    
    # Update logging level from config
    logging.getLogger().setLevel(getattr(logging, log_level))
    
    print(f"🔗 Using configuration:")
    print(f"   Service URL: {service_url}")
    print(f"   Topic: {topic}")
    print(f"   Auth: {auth_config}")
    print(f"   Log Level: {log_level}")
    print("─" * 50)
    
    producer = None
    try:
        print(f"🔗 Connecting to Pulsar at {service_url}...")
        
        if "localhost" in service_url:
            print("📋 Make sure Pulsar is running: pulsar standalone")
        else:
            print("📋 Make sure your Pulsar instance is accessible")
        print("─" * 50)
        
        # Create producer with configuration
        producer = PulsarFinancialMessageProducer(
            service_url=service_url,
            topic=topic,
            auth_params=auth_config,
            producer_config=PRODUCER_CONFIG
        )
        
        # Connect to Pulsar
        producer.connect()
        print("✅ Connected successfully!")
        
        # Create topic if it doesn't exist (for local Pulsar)
        if "localhost" in service_url:
            print("📝 Creating topic if needed...")
            try:
                import requests
                topic_url = f"http://localhost:8080/admin/v2/persistent/public/default/{topic.replace('persistent://public/default/', '')}"
                response = requests.put(topic_url)
                if response.status_code in [204, 409]:  # 204 = created, 409 = already exists
                    print("✅ Topic ready!")
                else:
                    print(f"⚠️ Topic creation response: {response.status_code}")
            except Exception as e:
                print(f"⚠️ Could not create topic via API: {e}")
                print("📝 Will try auto-creation during message send...")
        
        # Send sample messages
        print("Generating and sending sample financial messages...")
        logger.info("Sending sample financial messages...")
        
        for i in range(3):
            message_id = producer.send_sample_message()
            print(f"✅ Sent message {i+1}/3, ID: {message_id}")
            logger.info(f"Sent message {i+1}/3, ID: {message_id}")
        
        print("\n🎉 All messages sent successfully!")
        
        if "localhost" in service_url:
            print("📊 Check the Pulsar admin UI at: http://localhost:8080")
            print(f"📝 Topic used: {topic}")
            print("# Check if topic was created:")
            print("curl http://localhost:8080/admin/v2/persistent/public/default")
            print("# Get topic statistics (should show message count > 0):")
            print(f"curl http://localhost:8080/admin/v2/persistent/public/default/{topic}/stats")
        
        logger.info("All messages sent successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\n🔧 Troubleshooting:")
        
        if "localhost" in service_url:
            print("For Local Pulsar:")
            print("1. Is Pulsar running? Start with: pulsar standalone")
            print("2. Check if port 6650 is free: lsof -i :6650")
            print("3. Wait a moment after starting Pulsar before running this script")
            print("4. Check Pulsar logs for any startup errors")
            print("5. Try connecting to http://localhost:8080 in your browser")
        else:
            print("For Remote Pulsar:")
            print("1. Check network connectivity to the Pulsar service")
            print("2. Verify authentication configuration")
            print("3. Ensure the service URL is correct")
            print("4. Check if required ports are open")
        
        print("6. Verify configuration in ../config.py")
        
        logger.error(f"Error in main execution: {e}")
        raise
        
    finally:
        if producer:
            producer.disconnect()

if __name__ == "__main__":
    main() 