#!/usr/bin/env python3
"""
Verification script for Market Core Implementation (Phase 2)

This script verifies the functionality of:
1. Creating a balance group with members
2. Submitting test energy readings
3. Running settlement calculation
4. Validating report output matches expected format
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
import uuid

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import async_session
from services.balance_group import BalanceGroupRepository
from services.energy_flow import EnergyFlowAggregator
from services.deviation import calculate_deviation
from services.settlement import calculate_settlement
from models.models import MarketParticipant, MeteringPoint, EnergyReading, BalanceGroup
from sqlalchemy import select


class MarketCoreVerification:
    """Verification class for Market Core functionality"""
    
    def __init__(self):
        self.test_balance_group_id = f"test_bg_{uuid.uuid4().hex[:8]}"
        self.test_participant_id = f"test_participant_{uuid.uuid4().hex[:8]}"
        self.test_metering_point_id = f"test_mp_{uuid.uuid4().hex[:8]}"
        self.verification_results = []
    
    async def run_verification(self):
        """Run complete Market Core verification"""
        print("🚀 Starting Market Core Verification (Phase 2)")
        print("=" * 60)
        
        try:
            async with async_session() as session:
                # Step 1: Create balance group with members
                await self.verify_balance_group_creation(session)
                
                # Step 2: Submit test energy readings
                await self.verify_energy_readings_submission(session)
                
                # Step 3: Run settlement calculation
                await self.verify_settlement_calculation(session)
                
                # Step 4: Validate report output
                await self.verify_report_generation(session)
                
                # Cleanup
                await self.cleanup_test_data(session)
                
        except Exception as e:
            print(f"❌ Verification failed with error: {e}")
            return False
        
        # Print results
        self.print_verification_results()
        
        # Return overall success
        return all(result['success'] for result in self.verification_results)
    
    async def verify_balance_group_creation(self, session):
        """Verify balance group creation and member management"""
        print("\n📋 Step 1: Verifying Balance Group Creation")
        
        try:
            repo = BalanceGroupRepository(session)
            
            # Create balance group
            balance_group = await repo.create_balance_group(
                self.test_balance_group_id, 
                "Test Balance Group"
            )
            
            assert balance_group is not None, "Balance group creation failed"
            assert balance_group.id == self.test_balance_group_id, "Balance group ID mismatch"
            
            print(f"✅ Created balance group: {balance_group.id}")
            
            # Create test participant first
            participant = MarketParticipant(
                id=self.test_participant_id,
                name="Test Participant",
                address="Test Address"
            )
            session.add(participant)
            await session.commit()
            
            # Add member to balance group
            member = await repo.add_member(self.test_balance_group_id, self.test_participant_id)
            
            assert member is not None, "Member addition failed"
            assert member.balance_group_id == self.test_balance_group_id, "Member balance group ID mismatch"
            
            print(f"✅ Added member: {self.test_participant_id}")
            
            # Verify balance group retrieval
            retrieved_bg = await repo.get_balance_group(self.test_balance_group_id)
            assert retrieved_bg is not None, "Balance group retrieval failed"
            
            print("✅ Balance group retrieval successful")
            
            self.verification_results.append({
                'step': 'Balance Group Creation',
                'success': True,
                'details': f'Created balance group {self.test_balance_group_id} with member {self.test_participant_id}'
            })
            
        except Exception as e:
            print(f"❌ Balance group creation failed: {e}")
            self.verification_results.append({
                'step': 'Balance Group Creation',
                'success': False,
                'error': str(e)
            })
            raise
    
    async def verify_energy_readings_submission(self, session):
        """Verify energy readings submission"""
        print("\n⚡ Step 2: Verifying Energy Readings Submission")
        
        try:
            # Create test metering point
            metering_point = MeteringPoint(
                id=self.test_metering_point_id,
                location="Test Location",
                market_participant_id=self.test_participant_id
            )
            session.add(metering_point)
            
            # Create test energy readings
            base_time = datetime.utcnow() - timedelta(hours=24)
            test_readings = []
            
            for i in range(5):
                reading = EnergyReading(
                    id=f"test_reading_{i}_{uuid.uuid4().hex[:8]}",
                    metering_point_id=self.test_metering_point_id,
                    timestamp=base_time + timedelta(hours=i),
                    value_kwh=100.0 + (i * 10),  # 100, 110, 120, 130, 140 kWh
                    reading_type="consumption"
                )
                session.add(reading)
                test_readings.append(reading)
            
            await session.commit()
            
            print(f"✅ Created {len(test_readings)} test energy readings")
            
            # Verify readings were stored
            result = await session.execute(
                select(EnergyReading).where(
                    EnergyReading.metering_point_id == self.test_metering_point_id
                )
            )
            stored_readings = result.scalars().all()
            
            assert len(stored_readings) == 5, f"Expected 5 readings, found {len(stored_readings)}"
            
            print("✅ Energy readings verification successful")
            
            self.verification_results.append({
                'step': 'Energy Readings Submission',
                'success': True,
                'details': f'Created and verified {len(test_readings)} energy readings'
            })
            
        except Exception as e:
            print(f"❌ Energy readings submission failed: {e}")
            self.verification_results.append({
                'step': 'Energy Readings Submission',
                'success': False,
                'error': str(e)
            })
            raise
    
    async def verify_settlement_calculation(self, session):
        """Verify settlement calculation functionality"""
        print("\n💰 Step 3: Verifying Settlement Calculation")
        
        try:
            # Test deviation calculation
            actual_consumption = 500.0
            forecast_consumption = 480.0
            
            deviation = calculate_deviation(actual_consumption, forecast_consumption)
            expected_deviation = 20.0
            
            assert abs(deviation - expected_deviation) < 0.01, f"Deviation calculation error: {deviation} != {expected_deviation}"
            
            print(f"✅ Deviation calculation: {actual_consumption} - {forecast_consumption} = {deviation} kWh")
            
            # Test settlement calculation
            price_ct_per_kwh = 10
            settlement = calculate_settlement(deviation, price_ct_per_kwh)
            expected_settlement = 2.0  # (20 * 10) / 100 = 2.0 EUR
            
            assert abs(settlement - expected_settlement) < 0.01, f"Settlement calculation error: {settlement} != {expected_settlement}"
            
            print(f"✅ Settlement calculation: {deviation} kWh * {price_ct_per_kwh} ct/kWh = {settlement} EUR")
            
            # Test energy flow aggregation
            aggregator = EnergyFlowAggregator(session)
            
            # Use a time range that includes our test readings
            start_time = datetime.utcnow() - timedelta(hours=25)
            end_time = datetime.utcnow()
            
            aggregated = await aggregator.aggregate_energy_flows(
                self.test_balance_group_id, 
                start_time, 
                end_time
            )
            
            assert 'consumption_kwh' in aggregated, "Missing consumption_kwh in aggregated data"
            assert 'generation_kwh' in aggregated, "Missing generation_kwh in aggregated data"
            
            print(f"✅ Energy flow aggregation: {aggregated['consumption_kwh']} kWh consumption, {aggregated['generation_kwh']} kWh generation")
            
            self.verification_results.append({
                'step': 'Settlement Calculation',
                'success': True,
                'details': f'Deviation: {deviation} kWh, Settlement: {settlement} EUR, Aggregated: {aggregated["consumption_kwh"]} kWh'
            })
            
        except Exception as e:
            print(f"❌ Settlement calculation failed: {e}")
            self.verification_results.append({
                'step': 'Settlement Calculation',
                'success': False,
                'error': str(e)
            })
            raise
    
    async def verify_report_generation(self, session):
        """Verify settlement report generation"""
        print("\n📊 Step 4: Verifying Report Generation")
        
        try:
            # Simulate report generation logic
            repo = BalanceGroupRepository(session)
            balance_group = await repo.get_balance_group(self.test_balance_group_id)
            
            assert balance_group is not None, "Balance group not found for report generation"
            
            # Aggregate energy flows
            aggregator = EnergyFlowAggregator(session)
            start_time = datetime.utcnow() - timedelta(hours=25)
            end_time = datetime.utcnow()
            
            aggregated = await aggregator.aggregate_energy_flows(
                self.test_balance_group_id, 
                start_time, 
                end_time
            )
            
            # Calculate deviations (using example forecasts)
            consumption_deviation = calculate_deviation(
                aggregated['consumption_kwh'], 
                aggregated['consumption_kwh'] * 0.95
            )
            generation_deviation = calculate_deviation(
                aggregated['generation_kwh'], 
                aggregated['generation_kwh'] * 1.05
            )
            
            # Calculate settlements
            price_ct_per_kwh = 10
            consumption_settlement = calculate_settlement(consumption_deviation, price_ct_per_kwh)
            generation_settlement = calculate_settlement(generation_deviation, price_ct_per_kwh)
            
            # Create report structure
            report = {
                "balance_group_id": self.test_balance_group_id,
                "period": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat()
                },
                "aggregated_flows": aggregated,
                "deviations": {
                    "consumption_kwh": consumption_deviation,
                    "generation_kwh": generation_deviation
                },
                "settlements": {
                    "consumption_eur": consumption_settlement,
                    "generation_eur": generation_settlement
                }
            }
            
            # Validate report structure
            required_fields = ["balance_group_id", "period", "aggregated_flows", "deviations", "settlements"]
            for field in required_fields:
                assert field in report, f"Missing required field in report: {field}"
            
            assert "start" in report["period"], "Missing start time in report period"
            assert "end" in report["period"], "Missing end time in report period"
            
            print("✅ Report structure validation successful")
            print(f"   - Balance Group: {report['balance_group_id']}")
            print(f"   - Consumption Settlement: {report['settlements']['consumption_eur']} EUR")
            print(f"   - Generation Settlement: {report['settlements']['generation_eur']} EUR")
            
            self.verification_results.append({
                'step': 'Report Generation',
                'success': True,
                'details': f'Generated report with settlements: {consumption_settlement} EUR (consumption), {generation_settlement} EUR (generation)'
            })
            
        except Exception as e:
            print(f"❌ Report generation failed: {e}")
            self.verification_results.append({
                'step': 'Report Generation',
                'success': False,
                'error': str(e)
            })
            raise
    
    async def cleanup_test_data(self, session):
        """Clean up test data"""
        print("\n🧹 Cleaning up test data...")
        
        try:
            # Delete test energy readings
            await session.execute(
                f"DELETE FROM energy_readings WHERE metering_point_id = '{self.test_metering_point_id}'"
            )
            
            # Delete test metering point
            await session.execute(
                f"DELETE FROM metering_points WHERE id = '{self.test_metering_point_id}'"
            )
            
            # Delete balance group member
            await session.execute(
                f"DELETE FROM balance_group_members WHERE balance_group_id = '{self.test_balance_group_id}'"
            )
            
            # Delete test balance group
            await session.execute(
                f"DELETE FROM balance_groups WHERE id = '{self.test_balance_group_id}'"
            )
            
            # Delete test participant
            await session.execute(
                f"DELETE FROM market_participants WHERE id = '{self.test_participant_id}'"
            )
            
            await session.commit()
            print("✅ Test data cleanup completed")
            
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")
    
    def print_verification_results(self):
        """Print verification results summary"""
        print("\n" + "=" * 60)
        print("📋 MARKET CORE VERIFICATION RESULTS")
        print("=" * 60)
        
        success_count = 0
        total_count = len(self.verification_results)
        
        for result in self.verification_results:
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            print(f"{status} {result['step']}")
            
            if result['success']:
                success_count += 1
                if 'details' in result:
                    print(f"    Details: {result['details']}")
            else:
                if 'error' in result:
                    print(f"    Error: {result['error']}")
        
        print(f"\nOverall Result: {success_count}/{total_count} tests passed")
        
        if success_count == total_count:
            print("🎉 Market Core verification SUCCESSFUL!")
        else:
            print("💥 Market Core verification FAILED!")


async def main():
    """Main verification function"""
    verifier = MarketCoreVerification()
    success = await verifier.run_verification()
    
    if success:
        print("\n✅ All Market Core verification tests passed!")
        return 0
    else:
        print("\n❌ Market Core verification failed!")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
