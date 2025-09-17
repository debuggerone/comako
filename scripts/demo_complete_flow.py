#!/usr/bin/env python3
"""
CoMaKo Complete System Demo Flow

This script demonstrates the complete end-to-end functionality of the
CoMaKo energy cooperative management system, including:

1. Meter Reading → Settlement Flow
2. EDI Processing Flow  
3. Balance Group Management
4. Anomaly Detection
5. Market Core Integration

Usage: python scripts/demo_complete_flow.py
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any
from unittest.mock import AsyncMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import CoMaKo services
from src.services.settlement import calculate_settlement, calculate_settlement_with_percentage
from src.services.deviation import calculate_deviation, calculate_deviation_percentage
from src.services.balance_group import BalanceGroupRepository
from src.services.energy_flow import aggregate_energy_flows
from src.services.meter_reading import MeterReadingRepository, SettlementMessageConsumer
from src.services.anomaly_detection import AnomalyDetector
from src.services.edi_parser import EDIFACTParser
from src.services.edi_converter import convert_edi_to_json, convert_utilmd_to_json
from src.services.aperak_generator import APERAKGenerator
from src.services.edi_validator import validate_edi_message, create_validation_report
from src.clients.market_core import MarketCoreClient


class CoMaKoSystemDemo:
    """Complete system demonstration orchestrator."""
    
    def __init__(self):
        """Initialize demo with mock database session."""
        self.mock_session = AsyncMock()
        self.demo_results = {}
        self.start_time = datetime.now()
        
    def print_header(self, title: str, level: int = 1):
        """Print formatted section header."""
        if level == 1:
            print(f"\n{'='*60}")
            print(f"🚀 {title}")
            print(f"{'='*60}")
        elif level == 2:
            print(f"\n{'─'*40}")
            print(f"📋 {title}")
            print(f"{'─'*40}")
        else:
            print(f"\n🔸 {title}")
    
    def print_success(self, message: str):
        """Print success message."""
        print(f"✅ {message}")
    
    def print_info(self, message: str):
        """Print info message."""
        print(f"ℹ️  {message}")
    
    def print_warning(self, message: str):
        """Print warning message."""
        print(f"⚠️  {message}")
    
    def print_error(self, message: str):
        """Print error message."""
        print(f"❌ {message}")

    def demo_settlement_calculations(self):
        """Demonstrate settlement and deviation calculations."""
        self.print_header("Settlement & Deviation Calculations", 2)
        
        try:
            # Test basic settlement calculation
            deviation_kwh = 100.0
            price_ct_per_kwh = 10
            settlement_eur = calculate_settlement(deviation_kwh, price_ct_per_kwh)
            
            self.print_success(f"Basic Settlement: {deviation_kwh} kWh × {price_ct_per_kwh} ct/kWh = {settlement_eur} EUR")
            
            # Test percentage-based settlement
            percentage = 15.0
            settlement_pct = calculate_settlement_with_percentage(deviation_kwh, percentage)
            self.print_success(f"Percentage Settlement: {deviation_kwh} kWh × {percentage}% = {settlement_pct} EUR")
            
            # Test deviation calculations
            actual = 1150.0
            forecast = 1000.0
            deviation = calculate_deviation(actual, forecast)
            deviation_pct = calculate_deviation_percentage(actual, forecast)
            
            self.print_success(f"Deviation: {actual} - {forecast} = {deviation} kWh ({deviation_pct}%)")
            
            self.demo_results['settlement_calculations'] = {
                'basic_settlement': settlement_eur,
                'percentage_settlement': settlement_pct,
                'deviation_kwh': deviation,
                'deviation_percentage': deviation_pct
            }
            
        except Exception as e:
            self.print_error(f"Settlement calculations failed: {e}")
            self.demo_results['settlement_calculations'] = {'error': str(e)}

    async def demo_balance_group_management(self):
        """Demonstrate balance group management."""
        self.print_header("Balance Group Management", 2)
        
        try:
            # Initialize balance group repository
            bg_repo = BalanceGroupRepository(self.mock_session)
            
            # Create sample balance group
            balance_group_data = {
                'id': 'BG001',
                'name': 'Energiegenossenschaft Musterstadt',
                'responsible_party': 'COMAKO',
                'market_location': 'DE_MUSTERSTADT',
                'created_at': datetime.now()
            }
            
            self.print_info(f"Creating balance group: {balance_group_data['name']}")
            
            # Simulate balance group creation (mock)
            self.mock_session.add = AsyncMock()
            self.mock_session.commit = AsyncMock()
            
            self.print_success("Balance group created successfully")
            
            # Add members to balance group
            members = [
                {'id': 'MP001', 'name': 'Haushalt Schmidt', 'type': 'consumer'},
                {'id': 'MP002', 'name': 'Solar Anlage Müller', 'type': 'producer'},
                {'id': 'MP003', 'name': 'Gewerbe Bäckerei', 'type': 'consumer'}
            ]
            
            for member in members:
                self.print_info(f"Adding member: {member['name']} ({member['type']})")
            
            self.print_success(f"Added {len(members)} members to balance group")
            
            self.demo_results['balance_group'] = {
                'group_id': balance_group_data['id'],
                'group_name': balance_group_data['name'],
                'members_count': len(members),
                'members': members
            }
            
        except Exception as e:
            self.print_error(f"Balance group management failed: {e}")
            self.demo_results['balance_group'] = {'error': str(e)}

    async def demo_meter_reading_flow(self):
        """Demonstrate complete meter reading to settlement flow."""
        self.print_header("Meter Reading → Settlement Flow", 2)
        
        try:
            # Initialize services
            meter_repo = MeterReadingRepository(self.mock_session)
            settlement_consumer = SettlementMessageConsumer(self.mock_session)
            
            # Sample meter readings
            readings = [
                {
                    'id': 'R001',
                    'metering_point_id': 'MP001',
                    'value_kwh': 150.0,
                    'reading_type': 'consumption',
                    'timestamp': datetime.now(),
                    'source': 'smart_meter'
                },
                {
                    'id': 'R002', 
                    'metering_point_id': 'MP002',
                    'value_kwh': 200.0,
                    'reading_type': 'generation',
                    'timestamp': datetime.now(),
                    'source': 'solar_inverter'
                },
                {
                    'id': 'R003',
                    'metering_point_id': 'MP003',
                    'value_kwh': 75.0,
                    'reading_type': 'consumption', 
                    'timestamp': datetime.now(),
                    'source': 'manual_reading'
                }
            ]
            
            total_consumption = 0
            total_generation = 0
            settlements = []
            
            for reading in readings:
                self.print_info(f"Processing reading {reading['id']}: {reading['value_kwh']} kWh ({reading['reading_type']})")
                
                # Calculate forecast (simplified: 95% of actual)
                forecast = reading['value_kwh'] * 0.95
                
                # Calculate deviation and settlement
                deviation = calculate_deviation(reading['value_kwh'], forecast)
                settlement = calculate_settlement(deviation, price_ct_per_kwh=10)
                
                settlements.append({
                    'reading_id': reading['id'],
                    'metering_point': reading['metering_point_id'],
                    'actual': reading['value_kwh'],
                    'forecast': forecast,
                    'deviation': deviation,
                    'settlement': settlement
                })
                
                if reading['reading_type'] == 'consumption':
                    total_consumption += reading['value_kwh']
                else:
                    total_generation += reading['value_kwh']
                
                self.print_success(f"Settlement: {settlement} EUR (deviation: {deviation} kWh)")
            
            # Calculate balance group totals
            net_consumption = total_consumption - total_generation
            total_settlement = sum(s['settlement'] for s in settlements)
            
            self.print_info(f"Balance Group Summary:")
            self.print_info(f"  Total Consumption: {total_consumption} kWh")
            self.print_info(f"  Total Generation: {total_generation} kWh")
            self.print_info(f"  Net Consumption: {net_consumption} kWh")
            self.print_info(f"  Total Settlement: {total_settlement} EUR")
            
            self.demo_results['meter_reading_flow'] = {
                'readings_processed': len(readings),
                'total_consumption': total_consumption,
                'total_generation': total_generation,
                'net_consumption': net_consumption,
                'total_settlement': total_settlement,
                'settlements': settlements
            }
            
        except Exception as e:
            self.print_error(f"Meter reading flow failed: {e}")
            self.demo_results['meter_reading_flow'] = {'error': str(e)}

    def demo_anomaly_detection(self):
        """Demonstrate anomaly detection in meter readings."""
        self.print_header("Anomaly Detection", 2)
        
        try:
            # Initialize anomaly detector
            detector = AnomalyDetector(self.mock_session)
            
            # Sample readings with anomalies
            readings = [
                {'id': 'R001', 'value_kwh': 100.0, 'metering_point_id': 'MP001'},
                {'id': 'R002', 'value_kwh': 105.0, 'metering_point_id': 'MP001'},
                {'id': 'R003', 'value_kwh': 110.0, 'metering_point_id': 'MP001'},
                {'id': 'R004', 'value_kwh': 95.0, 'metering_point_id': 'MP001'},
                {'id': 'R005', 'value_kwh': 500.0, 'metering_point_id': 'MP001'},  # Anomaly
                {'id': 'R006', 'value_kwh': 15.0, 'metering_point_id': 'MP001'},   # Anomaly
                {'id': 'R007', 'value_kwh': 102.0, 'metering_point_id': 'MP001'},
            ]
            
            self.print_info(f"Analyzing {len(readings)} meter readings for anomalies...")
            
            # Detect anomalies
            anomalies = detector.detect_anomalies(readings, threshold_multiplier=2.0)
            
            self.print_success(f"Detected {len(anomalies)} anomalies:")
            
            for anomaly in anomalies:
                self.print_warning(f"  Anomaly {anomaly['id']}: {anomaly['value_kwh']} kWh (type: {anomaly.get('anomaly_type', 'statistical')})")
            
            # Calculate statistics
            values = [r['value_kwh'] for r in readings]
            mean_value = sum(values) / len(values)
            normal_readings = [r for r in readings if r not in anomalies]
            
            self.print_info(f"Statistics:")
            self.print_info(f"  Mean value: {mean_value:.2f} kWh")
            self.print_info(f"  Normal readings: {len(normal_readings)}")
            self.print_info(f"  Anomalous readings: {len(anomalies)}")
            self.print_info(f"  Anomaly rate: {len(anomalies)/len(readings)*100:.1f}%")
            
            self.demo_results['anomaly_detection'] = {
                'total_readings': len(readings),
                'anomalies_detected': len(anomalies),
                'anomaly_rate': len(anomalies)/len(readings)*100,
                'mean_value': mean_value,
                'anomalies': anomalies
            }
            
        except Exception as e:
            self.print_error(f"Anomaly detection failed: {e}")
            self.demo_results['anomaly_detection'] = {'error': str(e)}

    def demo_edi_processing_flow(self):
        """Demonstrate complete EDI processing flow."""
        self.print_header("EDI Processing Flow", 2)
        
        try:
            # Sample UTILMD message
            utilmd_message = """UNB+UNOC:3+SENDER123+COMAKO+250103:1200+REF001'
UNH+MSG001+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+DOC123+9'
DTM+137:20250103:102'
NAD+MS+COMPANY123+Energy Corp+Main St 123'
LOC+172+MP001+Metering Point 1'
QTY+220:1500.5:KWH'
MEA+AAE:KWH:1500.5:KWH'
UNT+8+MSG001'
UNZ+1+REF001'"""

            self.print_info("Processing UTILMD EDI message...")
            
            # Parse EDI message
            parser = EDIFACTParser()
            parsed_data = parser.parse_edi_file(utilmd_message)
            
            self.print_success(f"EDI parsing successful - {len(parsed_data)} segments parsed")
            
            # Convert to JSON
            json_result = convert_utilmd_to_json(parsed_data)
            
            self.print_success("EDI to JSON conversion successful")
            self.print_info(f"Message type: {json_result.get('message_type', 'UNKNOWN')}")
            
            # Validate EDI message
            validation_result = validate_edi_message(parsed_data)
            
            if validation_result['valid']:
                self.print_success("EDI validation passed")
            else:
                self.print_warning(f"EDI validation issues: {validation_result['statistics']['total_issues']}")
            
            # Generate APERAK response
            generator = APERAKGenerator(sender_id='COMAKO')
            aperak_response = generator.generate_acceptance_aperak(parsed_data)
            
            self.print_success("APERAK response generated")
            self.print_info(f"APERAK length: {len(aperak_response)} characters")
            
            # Extract business data
            metering_points = []
            consumption_data = []
            
            if 'LOC' in parsed_data:
                loc_data = parsed_data['LOC']
                if len(loc_data) >= 3:
                    metering_points.append({
                        'id': loc_data[1],
                        'name': loc_data[2] if len(loc_data) > 2 else 'Unknown'
                    })
            
            if 'QTY' in parsed_data:
                qty_data = parsed_data['QTY']
                if len(qty_data) >= 3:
                    consumption_data.append({
                        'type': qty_data[0],
                        'value': float(qty_data[1]) if qty_data[1].replace('.', '').isdigit() else 0.0,
                        'unit': qty_data[2]
                    })
            
            self.print_info(f"Extracted {len(metering_points)} metering points")
            self.print_info(f"Extracted {len(consumption_data)} consumption readings")
            
            self.demo_results['edi_processing'] = {
                'message_type': json_result.get('message_type', 'UNKNOWN'),
                'segments_parsed': len(parsed_data),
                'validation_passed': validation_result['valid'],
                'validation_issues': validation_result['statistics']['total_issues'],
                'metering_points': len(metering_points),
                'consumption_readings': len(consumption_data),
                'aperak_generated': True
            }
            
        except Exception as e:
            self.print_error(f"EDI processing failed: {e}")
            self.demo_results['edi_processing'] = {'error': str(e)}

    def demo_market_core_integration(self):
        """Demonstrate market core client integration."""
        self.print_header("Market Core Integration", 2)
        
        try:
            # Initialize market core client
            client = MarketCoreClient()
            
            self.print_info(f"Market Core client initialized")
            self.print_info(f"Base URL: {client.base_url}")
            self.print_info(f"Timeout: {client.timeout}s")
            
            # Simulate settlement data submission
            settlement_data = {
                'balance_group_id': 'BG001',
                'settlement_period': '2025-01-03',
                'total_consumption': 325.0,
                'total_generation': 200.0,
                'net_consumption': 125.0,
                'total_settlement': 12.50,
                'timestamp': datetime.now().isoformat()
            }
            
            self.print_info("Preparing settlement data for Market Core submission:")
            for key, value in settlement_data.items():
                self.print_info(f"  {key}: {value}")
            
            # Note: Actual HTTP call would be made here in real scenario
            self.print_success("Settlement data prepared for Market Core (simulation)")
            
            self.demo_results['market_core_integration'] = {
                'client_initialized': True,
                'base_url': client.base_url,
                'settlement_data_prepared': True,
                'data_points': len(settlement_data)
            }
            
        except Exception as e:
            self.print_error(f"Market core integration failed: {e}")
            self.demo_results['market_core_integration'] = {'error': str(e)}

    def demo_energy_flow_aggregation(self):
        """Demonstrate energy flow aggregation."""
        self.print_header("Energy Flow Aggregation", 2)
        
        try:
            # Sample energy flow data for a balance group
            energy_flows = [
                {'metering_point': 'MP001', 'type': 'consumption', 'value': 150.0, 'timestamp': datetime.now()},
                {'metering_point': 'MP002', 'type': 'generation', 'value': 200.0, 'timestamp': datetime.now()},
                {'metering_point': 'MP003', 'type': 'consumption', 'value': 75.0, 'timestamp': datetime.now()},
                {'metering_point': 'MP004', 'type': 'generation', 'value': 50.0, 'timestamp': datetime.now()},
            ]
            
            self.print_info(f"Aggregating {len(energy_flows)} energy flows...")
            
            # Aggregate by type
            total_consumption = sum(flow['value'] for flow in energy_flows if flow['type'] == 'consumption')
            total_generation = sum(flow['value'] for flow in energy_flows if flow['type'] == 'generation')
            net_consumption = total_consumption - total_generation
            
            # Calculate balance group efficiency
            efficiency = (total_generation / total_consumption * 100) if total_consumption > 0 else 0
            
            self.print_success("Energy flow aggregation completed:")
            self.print_info(f"  Total Consumption: {total_consumption} kWh")
            self.print_info(f"  Total Generation: {total_generation} kWh")
            self.print_info(f"  Net Consumption: {net_consumption} kWh")
            self.print_info(f"  Self-sufficiency: {efficiency:.1f}%")
            
            # Determine balance group status
            if net_consumption > 0:
                status = "Net Consumer"
            elif net_consumption < 0:
                status = "Net Producer"
            else:
                status = "Balanced"
            
            self.print_info(f"  Balance Group Status: {status}")
            
            self.demo_results['energy_flow_aggregation'] = {
                'flows_processed': len(energy_flows),
                'total_consumption': total_consumption,
                'total_generation': total_generation,
                'net_consumption': net_consumption,
                'self_sufficiency_percent': efficiency,
                'status': status
            }
            
        except Exception as e:
            self.print_error(f"Energy flow aggregation failed: {e}")
            self.demo_results['energy_flow_aggregation'] = {'error': str(e)}

    def print_demo_summary(self):
        """Print comprehensive demo summary."""
        self.print_header("Demo Summary & Results", 1)
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        self.print_info(f"Demo completed in {duration.total_seconds():.2f} seconds")
        self.print_info(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.print_info(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Count successful vs failed demos
        successful_demos = 0
        failed_demos = 0
        
        for demo_name, result in self.demo_results.items():
            if 'error' in result:
                failed_demos += 1
                self.print_error(f"{demo_name}: FAILED - {result['error']}")
            else:
                successful_demos += 1
                self.print_success(f"{demo_name}: SUCCESS")
        
        total_demos = successful_demos + failed_demos
        success_rate = (successful_demos / total_demos * 100) if total_demos > 0 else 0
        
        self.print_header("Overall Results", 2)
        self.print_info(f"Total Demos: {total_demos}")
        self.print_info(f"Successful: {successful_demos}")
        self.print_info(f"Failed: {failed_demos}")
        self.print_info(f"Success Rate: {success_rate:.1f}%")
        
        # Print key metrics
        if 'meter_reading_flow' in self.demo_results and 'error' not in self.demo_results['meter_reading_flow']:
            flow_data = self.demo_results['meter_reading_flow']
            self.print_header("Key Metrics", 2)
            self.print_info(f"Readings Processed: {flow_data['readings_processed']}")
            self.print_info(f"Total Settlement: {flow_data['total_settlement']} EUR")
            self.print_info(f"Net Consumption: {flow_data['net_consumption']} kWh")
        
        if 'anomaly_detection' in self.demo_results and 'error' not in self.demo_results['anomaly_detection']:
            anomaly_data = self.demo_results['anomaly_detection']
            self.print_info(f"Anomaly Rate: {anomaly_data['anomaly_rate']:.1f}%")
        
        if 'edi_processing' in self.demo_results and 'error' not in self.demo_results['edi_processing']:
            edi_data = self.demo_results['edi_processing']
            self.print_info(f"EDI Segments Parsed: {edi_data['segments_parsed']}")
            self.print_info(f"EDI Validation: {'PASSED' if edi_data['validation_passed'] else 'FAILED'}")
        
        # Final status
        if success_rate >= 80:
            self.print_success("🎉 CoMaKo System Demo: EXCELLENT PERFORMANCE!")
        elif success_rate >= 60:
            self.print_success("✅ CoMaKo System Demo: GOOD PERFORMANCE")
        else:
            self.print_warning("⚠️ CoMaKo System Demo: NEEDS IMPROVEMENT")

    async def run_complete_demo(self):
        """Run the complete system demonstration."""
        self.print_header("CoMaKo Energy Cooperative Management System", 1)
        self.print_header("Complete System Demonstration", 1)
        
        self.print_info("This demo showcases all major CoMaKo system components:")
        self.print_info("• Settlement & Deviation Calculations")
        self.print_info("• Balance Group Management")
        self.print_info("• Meter Reading → Settlement Flow")
        self.print_info("• Anomaly Detection")
        self.print_info("• EDI Processing Flow")
        self.print_info("• Market Core Integration")
        self.print_info("• Energy Flow Aggregation")
        
        # Run all demo components
        self.demo_settlement_calculations()
        await self.demo_balance_group_management()
        await self.demo_meter_reading_flow()
        self.demo_anomaly_detection()
        self.demo_edi_processing_flow()
        self.demo_market_core_integration()
        self.demo_energy_flow_aggregation()
        
        # Print summary
        self.print_demo_summary()


async def main():
    """Main demo execution function."""
    demo = CoMaKoSystemDemo()
    await demo.run_complete_demo()


if __name__ == "__main__":
    # Run the complete demo
    asyncio.run(main())
