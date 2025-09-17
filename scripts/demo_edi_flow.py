#!/usr/bin/env python3
"""
Demo Script: EDI Processing Flow

This script demonstrates the complete EDI processing flow:
1. Parse UTILMD/MSCONS messages
2. Convert to JSON format
3. Publish to message queue
4. Process business data
5. Generate APERAK responses
"""

import asyncio
import sys
import os
from datetime import datetime
from unittest.mock import AsyncMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.services.edi_parser import EDIFACTParser
from src.services.edi_converter import convert_edi_to_json, convert_utilmd_to_json, JSONValidator
from src.services.edi_processor import EDIProcessor, EDIMessageConsumer, process_and_publish_edi
from src.services.aperak_generator import APERAKGenerator, validate_aperak_message


async def demo_utilmd_processing():
    """Demonstrate UTILMD (Utilities Master Data) processing."""
    
    print("🚀 Starting UTILMD Processing Demo")
    print("=" * 60)
    
    # Step 1: Parse UTILMD message
    print("\n📄 Step 1: Parsing UTILMD message")
    
    sample_utilmd = '''UNB+UNOC:3+ENERGYCORP+COMAKO+250103:1200+REF001'
UNH+MSG001+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+DOC123+9'
DTM+137:20250103:102'
NAD+MS+ENERGYCORP+Energy Corporation+Main Street 123'
LOC+172+MP001+Metering Point Demo'
QTY+220:1500.5:KWH'
MEA+AAE:KWH:1500.5:KWH'
UNT+8+MSG001'
UNZ+1+REF001'
'''
    
    print(f"   📋 Raw UTILMD message:")
    print(f"   {sample_utilmd.strip()}")
    
    parser = EDIFACTParser()
    try:
        parsed_data = parser.parse_edi_file(sample_utilmd)
        print(f"   ✅ Parsing successful")
        print(f"   📊 Segments found: {list(parsed_data.keys())}")
    except Exception as e:
        print(f"   ❌ Parsing failed: {e}")
        return False
    
    # Step 2: Convert to JSON
    print("\n🔄 Step 2: Converting to JSON")
    
    try:
        json_result = convert_edi_to_json(parsed_data)
        utilmd_result = convert_utilmd_to_json(parsed_data)
        
        print(f"   ✅ JSON conversion successful")
        print(f"   📋 Message type: {json_result.get('message_type', 'UNKNOWN')}")
        print(f"   📊 Segments processed: {len(json_result.get('segments', []))}")
        
        # Validate JSON structure
        is_valid = JSONValidator.validate_basic_structure(json_result)
        utilmd_valid = JSONValidator.validate_utilmd_structure(utilmd_result)
        
        print(f"   ✅ Basic validation: {'PASSED' if is_valid else 'FAILED'}")
        print(f"   ✅ UTILMD validation: {'PASSED' if utilmd_valid else 'FAILED'}")
        
    except Exception as e:
        print(f"   ❌ JSON conversion failed: {e}")
        return False
    
    # Step 3: Process with EDI processor
    print("\n⚙️  Step 3: Processing with EDI processor")
    
    mock_session = AsyncMock()
    processor = EDIProcessor()
    consumer = EDIMessageConsumer(mock_session)
    
    try:
        # Extract business data
        message_id = processor._extract_message_id(parsed_data)
        sender_id = processor._extract_sender_id(parsed_data)
        recipient_id = processor._extract_recipient_id(parsed_data)
        
        print(f"   📋 Message ID: {message_id}")
        print(f"   📤 Sender: {sender_id}")
        print(f"   📥 Recipient: {recipient_id}")
        
        # Extract metering point data
        metering_points = consumer._extract_metering_points(parsed_data)
        consumption_data = consumer._extract_consumption_data(parsed_data)
        
        print(f"   📍 Metering points: {len(metering_points)}")
        print(f"   ⚡ Consumption readings: {len(consumption_data)}")
        
        if metering_points:
            print(f"   📊 First metering point: {metering_points[0]}")
        if consumption_data:
            print(f"   📈 First consumption: {consumption_data[0]}")
        
    except Exception as e:
        print(f"   ❌ Processing failed: {e}")
        return False
    
    # Step 4: Generate APERAK response
    print("\n📨 Step 4: Generating APERAK response")
    
    try:
        generator = APERAKGenerator(sender_id="COMAKO")
        
        # Generate acceptance APERAK
        acceptance_aperak = generator.generate_acceptance_aperak(parsed_data)
        validation_results = validate_aperak_message(acceptance_aperak)
        
        print(f"   ✅ APERAK generated")
        print(f"   📏 Length: {len(acceptance_aperak)} characters")
        print(f"   ✅ Structure valid: {validation_results['structure_valid']}")
        print(f"   ✅ Response code valid: {validation_results['response_code_valid']}")
        
        # Show sample APERAK
        print(f"   📄 Sample APERAK:")
        aperak_lines = acceptance_aperak.split("'")
        for line in aperak_lines[:3]:  # Show first 3 segments
            if line.strip():
                print(f"      {line.strip()}'")
        if len(aperak_lines) > 3:
            print(f"      ... ({len(aperak_lines)-3} more segments)")
        
    except Exception as e:
        print(f"   ❌ APERAK generation failed: {e}")
        return False
    
    return True


async def demo_mscons_processing():
    """Demonstrate MSCONS (Metered Services Consumption Report) processing."""
    
    print("\n🚀 Starting MSCONS Processing Demo")
    print("=" * 60)
    
    # Step 1: Parse MSCONS message
    print("\n📄 Step 1: Parsing MSCONS message")
    
    sample_mscons = '''UNB+UNOC:3+ENERGYCORP+COMAKO+250103:1400+REF002'
UNH+MSG002+MSCONS:D:03B:UN:EEG+1.1e'
BGM+E02+CONSUMPTION_REPORT+9'
DTM+137:20250103:102'
NAD+MS+ENERGYCORP+Energy Corporation'
LOC+172+MP001+Metering Point Demo'
QTY+220:2150.75:KWH'
DTM+163:20250103:102'
UNT+8+MSG002'
UNZ+1+REF002'
'''
    
    print(f"   📋 Raw MSCONS message:")
    print(f"   {sample_mscons.strip()}")
    
    parser = EDIFACTParser()
    try:
        parsed_data = parser.parse_edi_file(sample_mscons)
        print(f"   ✅ Parsing successful")
        print(f"   📊 Segments found: {list(parsed_data.keys())}")
    except Exception as e:
        print(f"   ❌ Parsing failed: {e}")
        return False
    
    # Step 2: Process consumption data
    print("\n⚙️  Step 2: Processing consumption data")
    
    mock_session = AsyncMock()
    consumer = EDIMessageConsumer(mock_session)
    
    try:
        # Extract consumption data
        consumption_data = consumer._extract_consumption_data(parsed_data)
        metering_points = consumer._extract_metering_points(parsed_data)
        
        print(f"   📍 Metering points: {len(metering_points)}")
        print(f"   ⚡ Consumption readings: {len(consumption_data)}")
        
        if consumption_data:
            reading = consumption_data[0]
            print(f"   📊 Consumption value: {reading['value']} {reading['unit']}")
            
            # Simulate settlement calculation
            from src.services.settlement import calculate_settlement
            from src.services.deviation import calculate_deviation
            
            actual_value = reading['value']
            forecast_value = actual_value * 0.88  # 12% under-forecast
            
            deviation = calculate_deviation(actual_value, forecast_value)
            settlement = calculate_settlement(deviation, price_ct_per_kwh=15)
            
            print(f"   📈 Forecast: {forecast_value:.2f} kWh")
            print(f"   📉 Deviation: {deviation:.2f} kWh")
            print(f"   💰 Settlement: {settlement:.2f} EUR")
        
    except Exception as e:
        print(f"   ❌ Processing failed: {e}")
        return False
    
    return True


async def demo_error_handling():
    """Demonstrate error handling for invalid EDI messages."""
    
    print("\n🚀 Starting Error Handling Demo")
    print("=" * 60)
    
    # Test with invalid EDI
    print("\n❌ Step 1: Testing invalid EDI message")
    
    invalid_edi = "INVALID+EDI+MESSAGE+WITHOUT+PROPER+STRUCTURE"
    
    parser = EDIFACTParser()
    try:
        parsed_data = parser.parse_edi_file(invalid_edi)
        print(f"   ❌ Error handling failed: Should have rejected invalid EDI")
        return False
    except Exception as e:
        print(f"   ✅ Error handling successful: {str(e)[:50]}...")
    
    # Test with malformed segments
    print("\n❌ Step 2: Testing malformed segments")
    
    malformed_edi = '''UNB+INCOMPLETE_SEGMENT
UNH+MISSING_ELEMENTS
INVALID_SEGMENT_TYPE+DATA'
'''
    
    try:
        parsed_data = parser.parse_edi_file(malformed_edi)
        print(f"   ⚠️  Parsing succeeded with warnings (graceful degradation)")
        print(f"   📊 Segments parsed: {list(parsed_data.keys())}")
    except Exception as e:
        print(f"   ✅ Error handling successful: {str(e)[:50]}...")
    
    # Test APERAK for rejected message
    print("\n📨 Step 3: Generating rejection APERAK")
    
    try:
        generator = APERAKGenerator(sender_id="COMAKO")
        
        # Create sample original message for rejection
        original_message = {
            'UNB': ['UNOC:3', 'ENERGYCORP', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'message_type': 'UTILMD'
        }
        
        # Generate rejection APERAK with errors
        errors = [
            {'code': '16', 'description': 'Invalid segment sequence'},
            {'code': '25', 'description': 'Missing required field'}
        ]
        
        rejection_aperak = generator.generate_rejection_aperak(original_message, errors)
        validation_results = validate_aperak_message(rejection_aperak)
        
        print(f"   ✅ Rejection APERAK generated")
        print(f"   📏 Length: {len(rejection_aperak)} characters")
        print(f"   ✅ Structure valid: {validation_results['structure_valid']}")
        print(f"   📋 Errors included: {len(errors)}")
        
    except Exception as e:
        print(f"   ❌ Rejection APERAK failed: {e}")
        return False
    
    return True


async def demo_complete_edi_pipeline():
    """Demonstrate the complete EDI processing pipeline."""
    
    print("\n🚀 Starting Complete EDI Pipeline Demo")
    print("=" * 60)
    
    # Step 1: End-to-end UTILMD processing
    print("\n🔄 Step 1: End-to-end UTILMD processing")
    
    utilmd_message = '''UNB+UNOC:3+TESTCORP+COMAKO+250103:1500+REF003'
UNH+MSG003+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+MASTER_DATA+9'
DTM+137:20250103:102'
NAD+MS+TESTCORP+Test Corporation'
LOC+172+MP002+Test Metering Point'
QTY+220:3250.25:KWH'
MEA+AAE:KWH:3250.25:KWH'
UNT+8+MSG003'
UNZ+1+REF003'
'''
    
    try:
        # Use the complete pipeline function
        success = await process_and_publish_edi(utilmd_message, "UTILMD")
        print(f"   ✅ Pipeline processing: {'SUCCESS' if success else 'FAILED'}")
        
        if success:
            print(f"   📤 Message published to queue: edi.utilmd.received")
            print(f"   ⚙️  Ready for consumer processing")
        
    except Exception as e:
        print(f"   ❌ Pipeline failed: {e}")
        return False
    
    # Step 2: Simulate message queue processing
    print("\n📨 Step 2: Simulating message queue processing")
    
    mock_session = AsyncMock()
    consumer = EDIMessageConsumer(mock_session)
    
    # Simulate message from queue
    queue_message = {
        "message_id": "MSG003",
        "message_type": "UTILMD",
        "sender_id": "TESTCORP",
        "recipient_id": "COMAKO",
        "timestamp": datetime.utcnow().isoformat(),
        "parsed_data": {
            'UNB': ['UNOC:3', 'TESTCORP', 'COMAKO', '250103:1500', 'REF003'],
            'UNH': ['MSG003', 'UTILMD', 'D', '03B'],
            'LOC': ['172', 'MP002', 'Test Metering Point'],
            'QTY': ['220', '3250.25', 'KWH'],
        },
        "event_type": "edi_message_received"
    }
    
    try:
        await consumer.process_edi_message(queue_message)
        print(f"   ✅ Message processing: SUCCESS")
        print(f"   📊 Business data extracted and processed")
        print(f"   📨 APERAK response generated")
        
    except Exception as e:
        print(f"   ❌ Message processing failed: {e}")
        return False
    
    return True


async def main():
    """Main demo function."""
    
    print("CoMaKo Energy Cooperative Management System")
    print("EDI Processing Flow Demo")
    print("=" * 60)
    
    demo_results = []
    
    try:
        # Run all demos
        print("\n🎯 Running EDI Processing Demos...")
        
        # Demo 1: UTILMD processing
        result1 = await demo_utilmd_processing()
        demo_results.append(("UTILMD Processing", result1))
        
        # Demo 2: MSCONS processing
        result2 = await demo_mscons_processing()
        demo_results.append(("MSCONS Processing", result2))
        
        # Demo 3: Error handling
        result3 = await demo_error_handling()
        demo_results.append(("Error Handling", result3))
        
        # Demo 4: Complete pipeline
        result4 = await demo_complete_edi_pipeline()
        demo_results.append(("Complete Pipeline", result4))
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 Demo Results Summary:")
        
        all_passed = True
        for demo_name, passed in demo_results:
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"   {status}: {demo_name}")
            if not passed:
                all_passed = False
        
        print("\n" + "=" * 60)
        if all_passed:
            print("🎉 All EDI demos completed successfully!")
            print("\n📋 Summary:")
            print("   • UTILMD message parsing and processing: Working")
            print("   • MSCONS message parsing and processing: Working")
            print("   • JSON conversion and validation: Working")
            print("   • APERAK response generation: Working")
            print("   • Error handling and validation: Working")
            print("   • Complete EDI pipeline: Working")
            print("\nNext steps:")
            print("1. Start RabbitMQ: docker compose up rabbitmq")
            print("2. Test with real EDI files")
            print("3. Monitor message queues")
        else:
            print("❌ Some EDI demos failed. Please check the implementation.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
