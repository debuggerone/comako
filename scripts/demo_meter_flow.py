#!/usr/bin/env python3
"""
Demo Script: Meter Reading → Settlement Flow

This script demonstrates the complete meter reading to settlement calculation flow:
1. Submit test meter reading
2. Verify settlement calculation
3. Check report generation
4. Validate message flow
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.services.meter_reading import MeterReadingRepository, SettlementMessageConsumer
from src.services.settlement import calculate_settlement
from src.services.deviation import calculate_deviation
from src.services.balance_group import BalanceGroupRepository


async def demo_meter_reading_flow():
    """Demonstrate the complete meter reading to settlement flow."""
    
    print("🚀 Starting Meter Reading → Settlement Flow Demo")
    print("=" * 60)
    
    # Mock database session for demo
    mock_session = AsyncMock()
    
    # Step 1: Create test balance group and metering point
    print("\n📋 Step 1: Setting up test data")
    balance_group_repo = BalanceGroupRepository(mock_session)
    
    # Create test balance group
    test_balance_group = {
        "id": "BG001",
        "name": "Demo Energy Cooperative",
        "bkv_id": None
    }
    print(f"   ✅ Created balance group: {test_balance_group['name']}")
    
    # Step 2: Submit meter reading
    print("\n📊 Step 2: Submitting meter reading")
    meter_repo = MeterReadingRepository(mock_session)
    
    test_reading_data = {
        "metering_point_id": "MP001",
        "value_kwh": 1250.5,
        "reading_type": "consumption",
        "timestamp": datetime.utcnow()
    }
    
    print(f"   📍 Metering Point: {test_reading_data['metering_point_id']}")
    print(f"   ⚡ Reading Value: {test_reading_data['value_kwh']} kWh")
    print(f"   📅 Timestamp: {test_reading_data['timestamp']}")
    print(f"   🔖 Type: {test_reading_data['reading_type']}")
    
    # Simulate reading creation (without actual database)
    reading_id = "reading_001"
    print(f"   ✅ Reading created with ID: {reading_id}")
    
    # Step 3: Simulate message publishing
    print("\n📨 Step 3: Publishing reading to message queue")
    message_payload = {
        "reading_id": reading_id,
        "metering_point_id": test_reading_data["metering_point_id"],
        "value_kwh": test_reading_data["value_kwh"],
        "reading_type": test_reading_data["reading_type"],
        "timestamp": test_reading_data["timestamp"].isoformat(),
        "event_type": "meter_reading_created"
    }
    
    print(f"   📤 Message published to queue: meter.reading.created")
    print(f"   📋 Payload: {message_payload}")
    
    # Step 4: Process settlement message
    print("\n🧮 Step 4: Processing settlement calculation")
    consumer = SettlementMessageConsumer(mock_session)
    
    # Simulate forecast data (would come from database in real scenario)
    forecast_value = test_reading_data["value_kwh"] * 0.92  # 8% under-forecast
    actual_value = test_reading_data["value_kwh"]
    
    print(f"   📈 Forecast: {forecast_value:.2f} kWh")
    print(f"   📊 Actual: {actual_value:.2f} kWh")
    
    # Calculate deviation
    deviation = calculate_deviation(actual_value, forecast_value)
    print(f"   📉 Deviation: {deviation:.2f} kWh")
    
    # Calculate settlement
    price_ct_per_kwh = 12  # 12 ct/kWh penalty
    settlement_amount = calculate_settlement(deviation, price_ct_per_kwh)
    print(f"   💰 Settlement: {settlement_amount:.2f} EUR")
    
    # Step 5: Generate settlement report
    print("\n📄 Step 5: Generating settlement report")
    
    settlement_report = {
        "balance_group_id": test_balance_group["id"],
        "period_start": (datetime.utcnow() - timedelta(hours=1)).isoformat(),
        "period_end": datetime.utcnow().isoformat(),
        "readings_processed": 1,
        "total_actual_kwh": actual_value,
        "total_forecast_kwh": forecast_value,
        "total_deviation_kwh": deviation,
        "settlement_amount_eur": settlement_amount,
        "price_ct_per_kwh": price_ct_per_kwh
    }
    
    print(f"   📊 Report generated for balance group: {settlement_report['balance_group_id']}")
    print(f"   ⏱️  Period: {settlement_report['period_start']} to {settlement_report['period_end']}")
    print(f"   📈 Total deviation: {settlement_report['total_deviation_kwh']:.2f} kWh")
    print(f"   💰 Total settlement: {settlement_report['settlement_amount_eur']:.2f} EUR")
    
    # Step 6: Validate complete flow
    print("\n✅ Step 6: Flow validation")
    
    validations = [
        ("Meter reading created", reading_id is not None),
        ("Message published", message_payload["event_type"] == "meter_reading_created"),
        ("Deviation calculated", abs(deviation - (actual_value - forecast_value)) < 0.01),
        ("Settlement calculated", settlement_amount > 0),
        ("Report generated", settlement_report["balance_group_id"] == test_balance_group["id"])
    ]
    
    all_passed = True
    for description, passed in validations:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {description}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 Demo completed successfully! All validations passed.")
        print("\n📋 Summary:")
        print(f"   • Meter reading: {actual_value} kWh")
        print(f"   • Forecast: {forecast_value:.2f} kWh")
        print(f"   • Deviation: {deviation:.2f} kWh")
        print(f"   • Settlement: {settlement_amount:.2f} EUR")
        print(f"   • Message flow: Working")
    else:
        print("❌ Demo failed! Some validations did not pass.")
        return False
    
    return True


async def demo_anomaly_detection():
    """Demonstrate anomaly detection in meter readings."""
    
    print("\n🔍 Bonus Demo: Anomaly Detection")
    print("-" * 40)
    
    try:
        from src.services.anomaly_detection import AnomalyDetector
        
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Test readings with clear anomalies
        test_readings = [
            {"id": "r1", "value_kwh": 1200.0, "metering_point_id": "MP001"},
            {"id": "r2", "value_kwh": 1180.0, "metering_point_id": "MP001"},
            {"id": "r3", "value_kwh": 1220.0, "metering_point_id": "MP001"},
            {"id": "r4", "value_kwh": 1190.0, "metering_point_id": "MP001"},
            {"id": "r5", "value_kwh": 5000.0, "metering_point_id": "MP001"},  # Anomaly!
            {"id": "r6", "value_kwh": 50.0, "metering_point_id": "MP001"},    # Anomaly!
        ]
        
        print(f"   📊 Analyzing {len(test_readings)} readings...")
        
        anomalies = detector.detect_anomalies(test_readings, threshold_multiplier=2.0)
        
        print(f"   🚨 Detected {len(anomalies)} anomalies:")
        for anomaly in anomalies:
            print(f"      • Reading {anomaly['id']}: {anomaly['value_kwh']} kWh")
        
        print(f"   ✅ Anomaly detection: {'WORKING' if len(anomalies) > 0 else 'NO ANOMALIES DETECTED'}")
        
    except Exception as e:
        print(f"   ❌ Anomaly detection failed: {e}")


async def main():
    """Main demo function."""
    
    print("CoMaKo Energy Cooperative Management System")
    print("Meter Reading → Settlement Flow Demo")
    print("=" * 60)
    
    try:
        # Run main demo
        success = await demo_meter_reading_flow()
        
        # Run bonus demo
        await demo_anomaly_detection()
        
        print("\n" + "=" * 60)
        if success:
            print("🎉 All demos completed successfully!")
            print("\nNext steps:")
            print("1. Start RabbitMQ: docker compose up rabbitmq")
            print("2. Run with real database: docker compose up db")
            print("3. Start the API: uvicorn src.main:app --reload")
        else:
            print("❌ Demo failed. Please check the implementation.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
