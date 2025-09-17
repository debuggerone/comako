"""
Unit tests for EDI@Energy Validator

Tests the comprehensive EDI validation functionality according to
EDI@Energy specification used in the German energy market.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.services.edi_validator import (
    EDIEnergyValidator,
    EDIValidationRule,
    StructuralValidationRule,
    MessageTypeValidationRule,
    DataElementValidationRule,
    BusinessRuleValidationRule,
    EDIValidationError,
    validate_edi_message,
    validate_edi_file,
    create_validation_report
)


class TestEDIValidationRule:
    """Test base EDI validation rule functionality."""
    
    def test_rule_initialization(self):
        """Test rule initialization with parameters."""
        rule = EDIValidationRule(
            rule_id="TEST_001",
            description="Test rule",
            severity="error",
            message_types=["UTILMD", "MSCONS"]
        )
        
        assert rule.rule_id == "TEST_001"
        assert rule.description == "Test rule"
        assert rule.severity == "error"
        assert rule.message_types == ["UTILMD", "MSCONS"]
    
    def test_rule_applies_to_message_type(self):
        """Test rule applicability to message types."""
        rule = EDIValidationRule(
            rule_id="TEST_001",
            description="Test rule",
            message_types=["UTILMD"]
        )
        
        assert rule.applies_to("UTILMD") is True
        assert rule.applies_to("MSCONS") is False
    
    def test_rule_applies_to_all_when_no_types_specified(self):
        """Test rule applies to all message types when none specified."""
        rule = EDIValidationRule(
            rule_id="TEST_001",
            description="Test rule"
        )
        
        assert rule.applies_to("UTILMD") is True
        assert rule.applies_to("MSCONS") is True
        assert rule.applies_to("APERAK") is True
    
    def test_base_validate_returns_empty_list(self):
        """Test base validate method returns empty list."""
        rule = EDIValidationRule(
            rule_id="TEST_001",
            description="Test rule"
        )
        
        result = rule.validate({})
        assert result == []


class TestStructuralValidationRule:
    """Test structural validation rule."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.rule = StructuralValidationRule()
    
    def test_valid_message_structure(self):
        """Test validation of valid message structure."""
        valid_message = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'UNT': ['3', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        issues = self.rule.validate(valid_message)
        assert len(issues) == 0
    
    def test_missing_required_segments(self):
        """Test validation with missing required segments."""
        invalid_message = {
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'UNT': ['2', 'MSG001']
        }
        
        issues = self.rule.validate(invalid_message)
        
        # Should find missing UNB and UNZ segments
        assert len(issues) == 2
        assert any(issue['message'] == 'Missing required segment: UNB' for issue in issues)
        assert any(issue['message'] == 'Missing required segment: UNZ' for issue in issues)
        assert all(issue['severity'] == 'error' for issue in issues)
    
    def test_segment_order_validation(self):
        """Test validation of segment order."""
        invalid_order_message = {
            'UNZ': ['1', 'REF001'],  # UNZ before UNB
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD', 'D', '03B'],
            'UNT': ['3', 'MSG001']
        }
        
        issues = self.rule.validate(invalid_order_message)
        
        # Should find segment order issue
        order_issues = [issue for issue in issues if 'order' in issue['message'].lower()]
        assert len(order_issues) == 1
        assert order_issues[0]['severity'] == 'warning'


class TestMessageTypeValidationRule:
    """Test message type specific validation rule."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.rule = MessageTypeValidationRule()
    
    def test_valid_utilmd_message(self):
        """Test validation of valid UTILMD message."""
        utilmd_message = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp']
        }
        
        issues = self.rule.validate(utilmd_message)
        assert len(issues) == 0
    
    def test_utilmd_missing_required_segments(self):
        """Test UTILMD validation with missing required segments."""
        incomplete_utilmd = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9']
            # Missing DTM and NAD
        }
        
        issues = self.rule.validate(incomplete_utilmd)
        
        # Should find missing DTM and NAD segments
        assert len(issues) == 2
        assert any('DTM' in issue['message'] for issue in issues)
        assert any('NAD' in issue['message'] for issue in issues)
    
    def test_valid_mscons_message(self):
        """Test validation of valid MSCONS message."""
        mscons_message = {
            'UNH': ['MSG001', 'MSCONS:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
            'LOC': ['172', 'MP001', 'Metering Point 1']
        }
        
        issues = self.rule.validate(mscons_message)
        assert len(issues) == 0
    
    def test_mscons_missing_required_segments(self):
        """Test MSCONS validation with missing required segments."""
        incomplete_mscons = {
            'UNH': ['MSG001', 'MSCONS:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9']
            # Missing DTM, NAD, and LOC
        }
        
        issues = self.rule.validate(incomplete_mscons)
        
        # Should find missing DTM, NAD, and LOC segments
        assert len(issues) == 3
        assert any('DTM' in issue['message'] for issue in issues)
        assert any('NAD' in issue['message'] for issue in issues)
        assert any('LOC' in issue['message'] for issue in issues)
    
    def test_valid_aperak_message(self):
        """Test validation of valid APERAK message."""
        aperak_message = {
            'UNH': ['MSG001', 'APERAK:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9']
        }
        
        issues = self.rule.validate(aperak_message)
        assert len(issues) == 0
    
    def test_aperak_missing_bgm_segment(self):
        """Test APERAK validation with missing BGM segment."""
        incomplete_aperak = {
            'UNH': ['MSG001', 'APERAK:D:03B:UN:EEG', '1.1e']
            # Missing BGM
        }
        
        issues = self.rule.validate(incomplete_aperak)
        
        # Should find missing BGM segment
        assert len(issues) == 1
        assert 'BGM' in issues[0]['message']
    
    def test_unknown_message_type(self):
        """Test validation with unknown message type."""
        unknown_message = {
            'UNH': ['MSG001', 'UNKNOWN:D:03B:UN:EEG', '1.1e']
        }
        
        issues = self.rule.validate(unknown_message)
        # Should not generate errors for unknown message types
        assert len(issues) == 0


class TestDataElementValidationRule:
    """Test data element validation rule."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.rule = DataElementValidationRule()
    
    def test_valid_unb_segment(self):
        """Test validation of valid UNB segment."""
        message_with_valid_unb = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001']
        }
        
        issues = self.rule.validate(message_with_valid_unb)
        assert len(issues) == 0
    
    def test_unb_insufficient_elements(self):
        """Test UNB validation with insufficient data elements."""
        message_with_short_unb = {
            'UNB': ['UNOC:3', 'SENDER123']  # Missing elements
        }
        
        issues = self.rule.validate(message_with_short_unb)
        
        assert len(issues) == 1
        assert 'insufficient data elements' in issues[0]['message']
        assert issues[0]['segment'] == 'UNB'
    
    def test_unb_invalid_syntax_identifier(self):
        """Test UNB validation with invalid syntax identifier."""
        message_with_invalid_syntax = {
            'UNB': ['INVALID:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001']
        }
        
        issues = self.rule.validate(message_with_invalid_syntax)
        
        syntax_issues = [issue for issue in issues if 'syntax identifier' in issue['message']]
        assert len(syntax_issues) == 1
        assert syntax_issues[0]['severity'] == 'warning'
    
    def test_unb_empty_sender_receiver(self):
        """Test UNB validation with empty sender/receiver IDs."""
        message_with_empty_ids = {
            'UNB': ['UNOC:3', '', '', '250103:1200', 'REF001']
        }
        
        issues = self.rule.validate(message_with_empty_ids)
        
        # Should find empty sender and receiver ID issues
        assert len(issues) == 2
        assert any('sender ID is empty' in issue['message'] for issue in issues)
        assert any('receiver ID is empty' in issue['message'] for issue in issues)
    
    def test_valid_unh_segment(self):
        """Test validation of valid UNH segment."""
        message_with_valid_unh = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e']
        }
        
        issues = self.rule.validate(message_with_valid_unh)
        assert len(issues) == 0
    
    def test_unh_insufficient_elements(self):
        """Test UNH validation with insufficient data elements."""
        message_with_short_unh = {
            'UNH': ['MSG001']  # Missing elements
        }
        
        issues = self.rule.validate(message_with_short_unh)
        
        assert len(issues) == 1
        assert 'insufficient data elements' in issues[0]['message']
        assert issues[0]['segment'] == 'UNH'
    
    def test_unh_empty_message_reference(self):
        """Test UNH validation with empty message reference."""
        message_with_empty_ref = {
            'UNH': ['', 'UTILMD:D:03B:UN:EEG', '1.1e']
        }
        
        issues = self.rule.validate(message_with_empty_ref)
        
        assert len(issues) == 1
        assert 'message reference number is empty' in issues[0]['message']
    
    def test_valid_dtm_segment(self):
        """Test validation of valid DTM segment."""
        message_with_valid_dtm = {
            'DTM': ['137', '20250103', '102']
        }
        
        issues = self.rule.validate(message_with_valid_dtm)
        assert len(issues) == 0
    
    def test_dtm_invalid_date_format(self):
        """Test DTM validation with invalid date format."""
        message_with_invalid_date = {
            'DTM': ['137', 'INVALID_DATE', '102']
        }
        
        issues = self.rule.validate(message_with_invalid_date)
        
        date_issues = [issue for issue in issues if 'date format' in issue['message']]
        assert len(date_issues) == 1
        assert date_issues[0]['severity'] == 'warning'
    
    def test_dtm_valid_date_formats(self):
        """Test DTM validation with various valid date formats."""
        valid_dates = ['20250103', '250103', '202501031200', '2025-01-03']
        
        for date_format in valid_dates:
            message = {'DTM': ['137', date_format, '102']}
            issues = self.rule.validate(message)
            
            date_issues = [issue for issue in issues if 'date format' in issue['message']]
            assert len(date_issues) == 0, f"Date format {date_format} should be valid"


class TestBusinessRuleValidationRule:
    """Test business rule validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.rule = BusinessRuleValidationRule()
    
    def test_valid_quantity_values(self):
        """Test validation of valid quantity values."""
        message_with_valid_qty = {
            'QTY': ['220', '1500.5', 'KWH']
        }
        
        issues = self.rule.validate(message_with_valid_qty)
        assert len(issues) == 0
    
    def test_negative_quantity_value(self):
        """Test validation with negative quantity value."""
        message_with_negative_qty = {
            'QTY': ['220', '-100.0', 'KWH']
        }
        
        issues = self.rule.validate(message_with_negative_qty)
        
        negative_issues = [issue for issue in issues if 'Negative quantity' in issue['message']]
        assert len(negative_issues) == 1
        assert negative_issues[0]['severity'] == 'warning'
    
    def test_large_quantity_value(self):
        """Test validation with unusually large quantity value."""
        message_with_large_qty = {
            'QTY': ['220', '2000000.0', 'KWH']
        }
        
        issues = self.rule.validate(message_with_large_qty)
        
        large_issues = [issue for issue in issues if 'large quantity' in issue['message']]
        assert len(large_issues) == 1
        assert large_issues[0]['severity'] == 'warning'
    
    def test_invalid_quantity_format(self):
        """Test validation with invalid quantity format."""
        message_with_invalid_qty = {
            'QTY': ['220', 'INVALID_NUMBER', 'KWH']
        }
        
        issues = self.rule.validate(message_with_invalid_qty)
        
        format_issues = [issue for issue in issues if 'Invalid quantity format' in issue['message']]
        assert len(format_issues) == 1
        assert format_issues[0]['severity'] == 'warning'
    
    def test_valid_measurement_values(self):
        """Test validation of valid measurement values."""
        message_with_valid_mea = {
            'MEA': ['AAE', 'KWH', '1500.5', 'KWH']
        }
        
        issues = self.rule.validate(message_with_valid_mea)
        assert len(issues) == 0
    
    def test_negative_measurement_value(self):
        """Test validation with negative measurement value."""
        message_with_negative_mea = {
            'MEA': ['AAE', 'KWH', '1500.5', '-100.0']
        }
        
        issues = self.rule.validate(message_with_negative_mea)
        
        negative_issues = [issue for issue in issues if 'Negative measurement' in issue['message']]
        assert len(negative_issues) == 1
        assert negative_issues[0]['severity'] == 'warning'
    
    def test_invalid_measurement_format(self):
        """Test validation with invalid measurement format."""
        message_with_invalid_mea = {
            'MEA': ['AAE', 'KWH', '1500.5', 'INVALID']
        }
        
        issues = self.rule.validate(message_with_invalid_mea)
        
        format_issues = [issue for issue in issues if 'Invalid measurement format' in issue['message']]
        assert len(format_issues) == 1
        assert format_issues[0]['severity'] == 'warning'
    
    def test_valid_location_codes(self):
        """Test validation of valid location codes."""
        message_with_valid_loc = {
            'LOC': ['172', 'MP001', 'Metering Point 1']
        }
        
        issues = self.rule.validate(message_with_valid_loc)
        assert len(issues) == 0
    
    def test_empty_location_id(self):
        """Test validation with empty location ID."""
        message_with_empty_loc = {
            'LOC': ['172', '', 'Metering Point 1']
        }
        
        issues = self.rule.validate(message_with_empty_loc)
        
        empty_issues = [issue for issue in issues if 'Location ID is empty' in issue['message']]
        assert len(empty_issues) == 1
        assert empty_issues[0]['severity'] == 'warning'


class TestEDIEnergyValidator:
    """Test main EDI@Energy validator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = EDIEnergyValidator()
    
    def test_validator_initialization(self):
        """Test validator initialization with default rules."""
        assert len(self.validator.rules) == 4
        assert isinstance(self.validator.rules[0], StructuralValidationRule)
        assert isinstance(self.validator.rules[1], MessageTypeValidationRule)
        assert isinstance(self.validator.rules[2], DataElementValidationRule)
        assert isinstance(self.validator.rules[3], BusinessRuleValidationRule)
    
    def test_add_custom_rule(self):
        """Test adding custom validation rule."""
        custom_rule = EDIValidationRule(
            rule_id="CUSTOM_001",
            description="Custom test rule"
        )
        
        initial_count = len(self.validator.rules)
        self.validator.add_rule(custom_rule)
        
        assert len(self.validator.rules) == initial_count + 1
        assert self.validator.rules[-1] == custom_rule
    
    def test_validate_valid_message(self):
        """Test validation of valid EDI message."""
        valid_message = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
            'LOC': ['172', 'MP001', 'Metering Point 1'],
            'QTY': ['220', '1500.5', 'KWH'],
            'UNT': ['7', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        result = self.validator.validate_message(valid_message)
        
        assert result['valid'] is True
        assert result['message_type'] == '1.1e'
        assert result['statistics']['errors'] == 0
        assert 'validated_at' in result
        assert 'validation_time_seconds' in result['statistics']
    
    def test_validate_invalid_message(self):
        """Test validation of invalid EDI message."""
        invalid_message = {
            'UNH': ['MSG002', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'QTY': ['220', 'INVALID_NUMBER', 'KWH'],
            'UNT': ['3', 'MSG002']
        }
        
        result = self.validator.validate_message(invalid_message)
        
        assert result['valid'] is False
        assert result['statistics']['errors'] > 0
        assert len(result['issues']) > 0
    
    def test_validation_statistics_tracking(self):
        """Test validation statistics tracking."""
        initial_stats = self.validator.get_validation_statistics()
        assert initial_stats['messages_validated'] == 0
        
        # Validate a message
        test_message = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e']
        }
        self.validator.validate_message(test_message)
        
        updated_stats = self.validator.get_validation_statistics()
        assert updated_stats['messages_validated'] == 1
        assert updated_stats['total_issues'] > 0
    
    def test_reset_statistics(self):
        """Test resetting validation statistics."""
        # Validate a message to generate stats
        test_message = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e']
        }
        self.validator.validate_message(test_message)
        
        # Reset statistics
        self.validator.reset_statistics()
        
        stats = self.validator.get_validation_statistics()
        assert stats['messages_validated'] == 0
        assert stats['total_issues'] == 0
        assert stats['errors'] == 0
        assert stats['warnings'] == 0
        assert stats['info'] == 0
    
    def test_rule_exception_handling(self):
        """Test handling of exceptions in validation rules."""
        # Create a mock rule that raises an exception
        mock_rule = Mock()
        mock_rule.applies_to.return_value = True
        mock_rule.validate.side_effect = Exception("Test exception")
        mock_rule.rule_id = "MOCK_RULE"
        
        self.validator.add_rule(mock_rule)
        
        test_message = {
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e']
        }
        
        result = self.validator.validate_message(test_message)
        
        # Should handle exception gracefully and add error issue
        system_errors = [issue for issue in result['issues'] if issue['segment'] == 'SYSTEM']
        assert len(system_errors) == 1
        assert 'Validation rule error' in system_errors[0]['message']


class TestConvenienceFunctions:
    """Test convenience functions for EDI validation."""
    
    def test_validate_edi_message_function(self):
        """Test validate_edi_message convenience function."""
        test_message = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'UNT': ['3', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        result = validate_edi_message(test_message)
        
        assert 'valid' in result
        assert 'message_type' in result
        assert 'issues' in result
        assert 'statistics' in result
    
    @patch('src.services.edi_validator.EDIFACTParser')
    def test_validate_edi_file_function_success(self, mock_parser_class):
        """Test validate_edi_file convenience function with successful parsing."""
        # Mock parser
        mock_parser = Mock()
        mock_parser.parse_edi_file.return_value = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'UNT': ['3', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        mock_parser_class.return_value = mock_parser
        
        file_content = "UNB+UNOC:3+SENDER123+COMAKO+250103:1200+REF001'"
        
        result = validate_edi_file(file_content)
        
        assert result['valid'] is True
        mock_parser.parse_edi_file.assert_called_once_with(file_content)
    
    @patch('src.services.edi_validator.EDIFACTParser')
    def test_validate_edi_file_function_parse_error(self, mock_parser_class):
        """Test validate_edi_file convenience function with parsing error."""
        # Mock parser to raise exception
        mock_parser = Mock()
        mock_parser.parse_edi_file.side_effect = Exception("Parse error")
        mock_parser_class.return_value = mock_parser
        
        file_content = "INVALID EDI CONTENT"
        
        result = validate_edi_file(file_content)
        
        assert result['valid'] is False
        assert len(result['issues']) == 1
        assert result['issues'][0]['rule_id'] == 'PARSE_ERROR'
        assert 'Failed to parse EDI file' in result['issues'][0]['message']
    
    def test_create_validation_report(self):
        """Test validation report creation."""
        validation_result = {
            'valid': False,
            'message_type': 'UTILMD',
            'validated_at': '2025-01-03T12:00:00',
            'statistics': {
                'total_issues': 3,
                'errors': 2,
                'warnings': 1,
                'info': 0,
                'validation_time_seconds': 0.123
            },
            'issues': [
                {
                    'rule_id': 'STRUCT_001',
                    'severity': 'error',
                    'segment': 'UNB',
                    'message': 'Missing required segment: UNB',
                    'description': 'Structural validation'
                },
                {
                    'rule_id': 'DATA_001',
                    'severity': 'warning',
                    'segment': 'DTM',
                    'message': 'Invalid date format',
                    'description': 'Data element validation'
                }
            ]
        }
        
        report = create_validation_report(validation_result)
        
        assert 'EDI@Energy Validation Report' in report
        assert 'Message Type: UTILMD' in report
        assert 'Validation Status: ❌ INVALID' in report
        assert 'Total Issues: 3' in report
        assert 'Errors: 2' in report
        assert 'Warnings: 1' in report
        assert 'Missing required segment: UNB' in report
        assert 'Invalid date format' in report
    
    def test_create_validation_report_valid_message(self):
        """Test validation report creation for valid message."""
        validation_result = {
            'valid': True,
            'message_type': 'UTILMD',
            'validated_at': '2025-01-03T12:00:00',
            'statistics': {
                'total_issues': 0,
                'errors': 0,
                'warnings': 0,
                'info': 0,
                'validation_time_seconds': 0.050
            },
            'issues': []
        }
        
        report = create_validation_report(validation_result)
        
        assert 'Validation Status: ✅ VALID' in report
        assert 'No issues found - message is valid!' in report


class TestEDIValidationIntegration:
    """Integration tests for EDI validation."""
    
    def test_complete_utilmd_validation_flow(self):
        """Test complete validation flow for UTILMD message."""
        utilmd_message = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
            'LOC': ['172', 'MP001', 'Metering Point 1'],
            'QTY': ['220', '1500.5', 'KWH'],
            'MEA': ['AAE', 'KWH', '1500.5', 'KWH'],
            'UNT': ['8', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        # Validate message
        result = validate_edi_message(utilmd_message)
        
        # Generate report
        report = create_validation_report(result)
        
        # Verify results
        assert result['valid'] is True
        assert result['message_type'] == '1.1e'
        assert 'UTILMD' in report
        assert 'VALID' in report
    
    def test_complete_validation_with_multiple_issues(self):
        """Test complete validation flow with multiple validation issues."""
        problematic_message = {
            'UNH': ['', 'UTILMD:D:03B:UN:EEG', '1.1e'],  # Empty message reference
            'QTY': ['220', '-100.0', 'KWH'],  # Negative quantity
            'MEA': ['AAE', 'KWH', '1500.5', 'INVALID'],  # Invalid measurement
            'LOC': ['172', '', 'Metering Point 1']  # Empty location ID
        }
        
        # Validate message
        result = validate_edi_message(problematic_message)
        
        # Generate report
        report = create_validation_report(result)
        
        # Verify multiple issues found
        assert result['valid'] is False
        assert result['statistics']['total_issues'] > 3
        assert result['statistics']['errors'] > 0
        assert result['statistics']['warnings'] > 0
        
        # Verify specific issues are detected
        issue_messages = [issue['message'] for issue in result['issues']]
        assert any('message reference number is empty' in msg for msg in issue_messages)
        assert any('Negative quantity' in msg for msg in issue_messages)
        assert any('Invalid measurement format' in msg for msg in issue_messages)
        assert any('Location ID is empty' in msg for msg in issue_messages)
        
        # Verify report contains issue details
        assert 'INVALID' in report
        assert 'message reference number is empty' in report


class TestEDIValidationPerformance:
    """Test EDI validation performance and edge cases."""
    
    def test_large_message_validation(self):
        """Test validation performance with large EDI message."""
        # Create a large message with many segments
        large_message = {
            'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
            'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
            'BGM': ['E01', 'DOC123', '9'],
            'DTM': ['137', '20250103', '102'],
            'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
            'UNT': ['100', 'MSG001'],
            'UNZ': ['1', 'REF001']
        }
        
        # Add many LOC and QTY segments
        for i in range(100):
            large_message[f'LOC_{i}'] = ['172', f'MP{i:03d}', f'Metering Point {i}']
            large_message[f'QTY_{i}'] = ['220', f'{1500.5 + i}', 'KWH']
        
        validator = EDIEnergyValidator()
        start_time = datetime.now()
        
        result = validator.validate_message(large_message)
        
        end_time = datetime.now()
        validation_time = (end_time - start_time).total_seconds()
        
        # Should complete validation in reasonable time (< 1 second)
        assert validation_time < 1.0
        assert 'validation_time_seconds' in result['statistics']
        assert result['statistics']['validation_time_seconds'] > 0
    
    def test_empty_message_validation(self):
        """Test validation of empty message."""
        empty_message = {}
        
        result = validate_edi_message(empty_message)
        
        assert result['valid'] is False
        assert result['statistics']['errors'] > 0
        
        # Should find missing required segments
        missing_segments = [issue for issue in result['issues'] 
                          if 'Missing required segment' in issue['message']]
        assert len(missing_segments) >= 4  # UNB, UNH, UNT, UNZ
    
    def test_malformed_segment_data(self):
        """Test validation with malformed segment data."""
        malformed_message = {
            'UNB': 'not_a_list',  # Should be list
            'UNH': [],  # Empty list
            'UNT': ['only_one_element'],  # Insufficient elements
            'UNZ': None  # None value
        }
        
        validator = EDIEnergyValidator()
        
        # Should not crash on malformed data
        result = validator.validate_message(malformed_message)
        
        assert result['valid'] is False
        assert len(result['issues']) > 0


if __name__ == "__main__":
    # Run a quick test to verify the module works
    print("Running EDI@Energy Validator tests...")
    
    # Test basic functionality
    test_message = {
        'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
        'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
        'BGM': ['E01', 'DOC123', '9'],
        'DTM': ['137', '20250103', '102'],
        'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
        'LOC': ['172', 'MP001', 'Metering Point 1'],
        'QTY': ['220', '1500.5', 'KWH'],
        'UNT': ['7', 'MSG001'],
        'UNZ': ['1', 'REF001']
    }
    
    result = validate_edi_message(test_message)
    print(f"✅ Test validation result: Valid={result['valid']}, Issues={result['statistics']['total_issues']}")
    
    # Test report generation
    report = create_validation_report(result)
    print(f"✅ Test report generated: {len(report)} characters")
    
    print("EDI@Energy Validator tests completed successfully!")
