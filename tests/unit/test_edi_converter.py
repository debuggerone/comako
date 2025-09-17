import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
from datetime import datetime
from src.services.edi_converter import (
    EDIConverter, 
    convert_edi_to_json, 
    convert_utilmd_to_json, 
    convert_mscons_to_json,
    JSONValidator,
    pretty_print_json
)


class TestEDIConverter:
    """Test suite for EDI to JSON conversion functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.converter = EDIConverter()
        
        # Sample EDI data for testing
        self.sample_edi_data = {
            'UNB': ['UNOC', 'SENDER123', 'RECIPIENT456', '20250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp', 'Main St 123'],
            'LOC': ['172', 'MP001', 'Metering Point 1'],
            'QTY': ['220', '1500.5', 'KWH'],
            'MEA': ['AAE', 'KWH', '1500.5', 'KWH'],
            'UNT': ['8', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        self.mscons_edi_data = {
            'UNB': ['UNOC', 'SENDER123', 'RECIPIENT456', '20250103:1200', 'REF001'],
            'UNH': ['MSG002', 'MSCONS', 'D', '03B'],
            'BGM': ['E02', 'REPORT123', '9'],
            'DTM': ['137', '20250103', '102'],
            'QTY': ['220', '2500.0', 'KWH'],
            'MEA': ['AAE', 'KWH', '2500.0', 'KWH'],
            'UNT': ['6', 'MSG002'],
            'UNZ': ['1', 'REF001']
        }
    
    def test_converter_initialization(self):
        """Test EDIConverter initialization"""
        converter = EDIConverter()
        assert converter is not None
        assert hasattr(converter, 'segment_mappings')
        assert len(converter.segment_mappings) == 10
        
        # Check that all expected segment types are mapped
        expected_segments = ['UNB', 'UNH', 'BGM', 'DTM', 'NAD', 'LOC', 'QTY', 'MEA', 'UNT', 'UNZ']
        for segment in expected_segments:
            assert segment in converter.segment_mappings
    
    def test_basic_edi_conversion(self):
        """Test basic EDI to JSON conversion"""
        result = self.converter.convert_to_json(self.sample_edi_data)
        
        # Check basic structure
        assert isinstance(result, dict)
        assert 'message_type' in result
        assert 'timestamp' in result
        assert 'header' in result
        assert 'body' in result
        assert 'segments' in result
        assert 'metadata' in result
        
        # Check message type detection
        assert result['message_type'] == 'UTILMD'
        
        # Check segments processing
        assert len(result['segments']) == 10
        
        # Check metadata
        assert result['metadata']['conversion_version'] == '1.0'
        assert result['metadata']['source_format'] == 'EDIFACT'
    
    def test_message_type_determination(self):
        """Test message type determination from UNH segment"""
        # Test with UTILMD
        result = self.converter.convert_to_json(self.sample_edi_data)
        assert result['message_type'] == 'UTILMD'
        
        # Test with MSCONS
        result = self.converter.convert_to_json(self.mscons_edi_data)
        assert result['message_type'] == 'MSCONS'
        
        # Test with missing UNH
        edi_data_no_unh = {k: v for k, v in self.sample_edi_data.items() if k != 'UNH'}
        result = self.converter.convert_to_json(edi_data_no_unh)
        assert result['message_type'] == 'UNKNOWN'
    
    def test_unb_segment_conversion(self):
        """Test UNB (Interchange Header) segment conversion"""
        unb_data = ['UNOC', 'SENDER123', 'RECIPIENT456', '20250103:1200', 'REF001']
        result = self.converter._convert_unb_segment(unb_data)
        
        expected = {
            "interchange_header": {
                "syntax_identifier": "UNOC",
                "sender": "SENDER123",
                "recipient": "RECIPIENT456",
                "date_time": "20250103:1200",
                "control_reference": "REF001"
            }
        }
        assert result == expected
    
    def test_unh_segment_conversion(self):
        """Test UNH (Message Header) segment conversion"""
        unh_data = ['MSG001', 'UTILMD', 'D', '03B']
        result = self.converter._convert_unh_segment(unh_data)
        
        expected = {
            "message_header": {
                "reference_number": "MSG001",
                "message_type": "UTILMD",
                "version": "D",
                "release": "03B"
            }
        }
        assert result == expected
    
    def test_qty_segment_conversion(self):
        """Test QTY (Quantity) segment conversion"""
        qty_data = ['220', '1500.5', 'KWH']
        result = self.converter._convert_qty_segment(qty_data)
        
        expected = {
            "quantity": {
                "qualifier": "220",
                "value": 1500.5,
                "unit": "KWH"
            }
        }
        assert result == expected
    
    def test_mea_segment_conversion(self):
        """Test MEA (Measurement) segment conversion"""
        mea_data = ['AAE', 'KWH', '1500.5', 'KWH']
        result = self.converter._convert_mea_segment(mea_data)
        
        expected = {
            "measurement": {
                "qualifier": "AAE",
                "dimension": "KWH",
                "value": 1500.5,
                "unit": "KWH"
            }
        }
        assert result == expected
    
    def test_loc_segment_conversion(self):
        """Test LOC (Location) segment conversion"""
        loc_data = ['172', 'MP001', 'Metering Point 1']
        result = self.converter._convert_loc_segment(loc_data)
        
        expected = {
            "location": {
                "qualifier": "172",
                "identification": "MP001",
                "description": "Metering Point 1"
            }
        }
        assert result == expected
    
    def test_nad_segment_conversion(self):
        """Test NAD (Name and Address) segment conversion"""
        nad_data = ['MS', 'COMPANY123', 'Energy Corp', 'Main St 123']
        result = self.converter._convert_nad_segment(nad_data)
        
        expected = {
            "party_info": {
                "qualifier": "MS",
                "identification": "COMPANY123",
                "name": "Energy Corp",
                "address": "Main St 123"
            }
        }
        assert result == expected
    
    def test_dtm_segment_conversion(self):
        """Test DTM (Date/Time) segment conversion"""
        dtm_data = ['137', '20250103', '102']
        result = self.converter._convert_dtm_segment(dtm_data)
        
        expected = {
            "date_time": {
                "qualifier": "137",
                "date": "20250103",
                "format": "102"
            }
        }
        assert result == expected
    
    def test_unknown_segment_handling(self):
        """Test handling of unknown segment types"""
        edi_data_with_unknown = self.sample_edi_data.copy()
        edi_data_with_unknown['XYZ'] = ['unknown', 'segment', 'data']
        
        result = self.converter.convert_to_json(edi_data_with_unknown)
        
        # Find the unknown segment in results
        unknown_segment = None
        for segment in result['segments']:
            if segment['segment_type'] == 'XYZ':
                unknown_segment = segment
                break
        
        assert unknown_segment is not None
        assert unknown_segment['status'] == 'unmapped'
        assert unknown_segment['data'] == ['unknown', 'segment', 'data']
    
    def test_segment_categorization(self):
        """Test that segments are properly categorized into header, body, and metadata"""
        result = self.converter.convert_to_json(self.sample_edi_data)
        
        # Check header segments
        assert 'interchange_header' in result['header']
        assert 'message_header' in result['header']
        assert 'document_info' in result['header']
        
        # Check body segments
        assert 'date_time' in result['body']
        assert 'party_info' in result['body']
        assert 'location' in result['body']
        assert 'quantity' in result['body']
        assert 'measurement' in result['body']
        
        # Check metadata segments
        assert 'message_trailer' in result['metadata']
        assert 'interchange_trailer' in result['metadata']
    
    def test_error_handling(self):
        """Test error handling for invalid input"""
        with pytest.raises(ValueError):
            # This should raise an error due to invalid data structure
            self.converter.convert_to_json(None)
    
    def test_empty_segment_data(self):
        """Test handling of empty or malformed segment data"""
        # Test with empty list
        result = self.converter._convert_qty_segment([])
        assert result == {"quantity": []}
        
        # Test with insufficient data
        result = self.converter._convert_qty_segment(['220'])
        expected = {
            "quantity": {
                "qualifier": "220",
                "value": None,
                "unit": None
            }
        }
        assert result == expected


class TestConvenienceFunctions:
    """Test suite for convenience functions"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.sample_edi_data = {
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'LOC': ['172', 'MP001', 'Metering Point 1'],
            'QTY': ['220', '1500.5', 'KWH'],
            'MEA': ['AAE', 'KWH', '1500.5', 'KWH']
        }
        
        self.mscons_edi_data = {
            'UNH': ['MSG002', 'MSCONS', 'D', '03B'],
            'DTM': ['137', '20250103', '102'],
            'QTY': ['220', '2500.0', 'KWH'],
            'MEA': ['AAE', 'KWH', '2500.0', 'KWH']
        }
    
    def test_convert_edi_to_json_function(self):
        """Test the convenience function for basic EDI conversion"""
        result = convert_edi_to_json(self.sample_edi_data)
        
        assert isinstance(result, dict)
        assert 'message_type' in result
        assert 'segments' in result
        assert result['message_type'] == 'UTILMD'
    
    def test_convert_utilmd_to_json_function(self):
        """Test UTILMD-specific conversion function"""
        result = convert_utilmd_to_json(self.sample_edi_data)
        
        assert isinstance(result, dict)
        assert result['message_type'] == 'UTILMD'
        assert 'utilities_data' in result
        
        utilities_data = result['utilities_data']
        assert 'metering_points' in utilities_data
        assert 'consumption_data' in utilities_data
        assert 'meter_readings' in utilities_data
        
        # Check that data was extracted correctly
        assert len(utilities_data['metering_points']) == 1
        assert len(utilities_data['consumption_data']) == 1
        assert len(utilities_data['meter_readings']) == 1
    
    def test_convert_mscons_to_json_function(self):
        """Test MSCONS-specific conversion function"""
        result = convert_mscons_to_json(self.mscons_edi_data)
        
        assert isinstance(result, dict)
        assert result['message_type'] == 'MSCONS'
        assert 'consumption_report' in result
        
        consumption_report = result['consumption_report']
        assert 'reporting_period' in consumption_report
        assert 'meter_readings' in consumption_report
        assert 'consumption_totals' in consumption_report
        
        # Check that data was extracted correctly
        assert len(consumption_report['consumption_totals']) == 1
        assert len(consumption_report['meter_readings']) == 1
    
    def test_pretty_print_json_function(self):
        """Test JSON pretty printing function"""
        test_data = {"test": "data", "number": 123}
        result = pretty_print_json(test_data)
        
        assert isinstance(result, str)
        assert '"test": "data"' in result
        assert '"number": 123' in result
        
        # Test with custom indentation
        result_custom = pretty_print_json(test_data, indent=4)
        assert isinstance(result_custom, str)
        # Should have more spaces with indent=4
        assert len(result_custom) >= len(result)


class TestJSONValidator:
    """Test suite for JSON validation functionality"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.valid_basic_json = {
            "message_type": "UTILMD",
            "timestamp": "2025-01-03T12:00:00",
            "header": {},
            "body": {},
            "segments": [],
            "metadata": {}
        }
        
        self.valid_utilmd_json = {
            "message_type": "UTILMD",
            "timestamp": "2025-01-03T12:00:00",
            "header": {},
            "body": {},
            "segments": [],
            "metadata": {},
            "utilities_data": {
                "metering_points": [],
                "consumption_data": [],
                "meter_readings": []
            }
        }
        
        self.valid_mscons_json = {
            "message_type": "MSCONS",
            "timestamp": "2025-01-03T12:00:00",
            "header": {},
            "body": {},
            "segments": [],
            "metadata": {},
            "consumption_report": {
                "reporting_period": {},
                "meter_readings": [],
                "consumption_totals": []
            }
        }
    
    def test_validate_basic_structure_valid(self):
        """Test validation of valid basic JSON structure"""
        result = JSONValidator.validate_basic_structure(self.valid_basic_json)
        assert result is True
    
    def test_validate_basic_structure_invalid(self):
        """Test validation of invalid basic JSON structure"""
        # Missing required field
        invalid_json = self.valid_basic_json.copy()
        del invalid_json['message_type']
        
        result = JSONValidator.validate_basic_structure(invalid_json)
        assert result is False
        
        # Empty dict
        result = JSONValidator.validate_basic_structure({})
        assert result is False
    
    def test_validate_utilmd_structure_valid(self):
        """Test validation of valid UTILMD JSON structure"""
        result = JSONValidator.validate_utilmd_structure(self.valid_utilmd_json)
        assert result is True
    
    def test_validate_utilmd_structure_invalid(self):
        """Test validation of invalid UTILMD JSON structure"""
        # Wrong message type
        invalid_json = self.valid_utilmd_json.copy()
        invalid_json['message_type'] = 'MSCONS'
        
        result = JSONValidator.validate_utilmd_structure(invalid_json)
        assert result is False
        
        # Missing utilities_data
        invalid_json = self.valid_basic_json.copy()
        invalid_json['message_type'] = 'UTILMD'
        
        result = JSONValidator.validate_utilmd_structure(invalid_json)
        assert result is False
    
    def test_validate_mscons_structure_valid(self):
        """Test validation of valid MSCONS JSON structure"""
        result = JSONValidator.validate_mscons_structure(self.valid_mscons_json)
        assert result is True
    
    def test_validate_mscons_structure_invalid(self):
        """Test validation of invalid MSCONS JSON structure"""
        # Wrong message type
        invalid_json = self.valid_mscons_json.copy()
        invalid_json['message_type'] = 'UTILMD'
        
        result = JSONValidator.validate_mscons_structure(invalid_json)
        assert result is False
        
        # Missing consumption_report
        invalid_json = self.valid_basic_json.copy()
        invalid_json['message_type'] = 'MSCONS'
        
        result = JSONValidator.validate_mscons_structure(invalid_json)
        assert result is False


class TestIntegrationScenarios:
    """Test suite for integration scenarios"""
    
    def test_full_utilmd_conversion_pipeline(self):
        """Test complete UTILMD conversion pipeline"""
        # Simulate realistic UTILMD data
        utilmd_data = {
            'UNB': ['UNOC:3', 'SENDER+123', 'RECIPIENT+456', '250103:1200+01', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG+1.1e'],
            'BGM': ['E01', 'DOC123456', '9'],
            'DTM': ['137:20250103:102'],
            'NAD': ['MS', 'COMPANY123::9', 'Energy Cooperative Ltd', 'Main Street 123+12345+City'],
            'LOC': ['172', 'DE0001234567890123456789012345', 'Primary Metering Point'],
            'QTY': ['220:1500.5:KWH'],
            'MEA': ['AAE:KWH:1500.5:KWH'],
            'UNT': ['8', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        # Convert to JSON
        result = convert_utilmd_to_json(utilmd_data)
        
        # Validate structure
        assert JSONValidator.validate_utilmd_structure(result)
        
        # Check specific data extraction
        utilities_data = result['utilities_data']
        assert len(utilities_data['metering_points']) == 1
        assert utilities_data['metering_points'][0]['identification'] == 'DE0001234567890123456789012345'
        
        assert len(utilities_data['consumption_data']) == 1
        assert utilities_data['consumption_data'][0]['value'] == 1500.5
        
        assert len(utilities_data['meter_readings']) == 1
        assert utilities_data['meter_readings'][0]['value'] == 1500.5
    
    def test_full_mscons_conversion_pipeline(self):
        """Test complete MSCONS conversion pipeline"""
        # Simulate realistic MSCONS data
        mscons_data = {
            'UNB': ['UNOC:3', 'SENDER+123', 'RECIPIENT+456', '250103:1200+01', 'REF002'],
            'UNH': ['MSG002', 'MSCONS:D:03B:UN:EEG+1.1e'],
            'BGM': ['E02', 'REPORT789', '9'],
            'DTM': ['137:20250103:102'],
            'QTY': ['220:2500.0:KWH'],
            'QTY': ['220:500.0:KWH'],  # Additional consumption
            'MEA': ['AAE:KWH:3000.0:KWH'],
            'UNT': ['7', 'MSG002'],
            'UNZ': ['1', 'REF002']
        }
        
        # Convert to JSON
        result = convert_mscons_to_json(mscons_data)
        
        # Validate structure
        assert JSONValidator.validate_mscons_structure(result)
        
        # Check specific data extraction
        consumption_report = result['consumption_report']
        assert 'reporting_period' in consumption_report
        assert len(consumption_report['consumption_totals']) >= 1
        assert len(consumption_report['meter_readings']) == 1
    
    def test_error_recovery_and_logging(self):
        """Test error recovery and logging functionality"""
        # Test with malformed data that should still partially convert
        malformed_data = {
            'UNH': ['MSG001'],  # Incomplete UNH
            'QTY': ['220'],     # Incomplete QTY
            'INVALID': 'not_a_list',  # Invalid format
            'LOC': ['172', 'MP001', 'Valid Location']  # Valid segment
        }
        
        converter = EDIConverter()
        result = converter.convert_to_json(malformed_data)
        
        # Should still produce valid basic structure
        assert JSONValidator.validate_basic_structure(result)
        
        # Should handle the valid segment correctly
        loc_segment = None
        for segment in result['segments']:
            if segment['segment_type'] == 'LOC':
                loc_segment = segment
                break
        
        assert loc_segment is not None
        assert 'location' in loc_segment['data']
        assert loc_segment['data']['location']['identification'] == 'MP001'


if __name__ == "__main__":
    pytest.main([__file__])
