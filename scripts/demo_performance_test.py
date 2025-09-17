#!/usr/bin/env python3
"""
CoMaKo Performance & Stress Test Suite

This script performs comprehensive performance testing of the CoMaKo system
including load testing, stress testing, and performance benchmarking.

Usage: python scripts/demo_performance_test.py
"""

import asyncio
import sys
import os
import time
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any
from unittest.mock import AsyncMock
import concurrent.futures
import threading

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import CoMaKo services
from src.services.settlement import calculate_settlement, calculate_settlement_with_percentage
from src.services.deviation import calculate_deviation, calculate_deviation_percentage
from src.services.balance_group import BalanceGroupRepository
from src.services.meter_reading import MeterReadingRepository, SettlementMessageConsumer
from src.services.anomaly_detection import AnomalyDetector
from src.services.edi_parser import EDIFACTParser
from src.services.edi_converter import convert_edi_to_json, convert_utilmd_to_json
from src.services.aperak_generator import APERAKGenerator
from src.services.edi_validator import validate_edi_message


class CoMaKoPerformanceTest:
    """Performance and stress testing orchestrator."""
    
    def __init__(self):
        """Initialize performance test suite."""
        self.mock_session = AsyncMock()
        self.test_results = {}
        self.start_time = datetime.now()
        
    def print_header(self, title: str, level: int = 1):
        """Print formatted section header."""
        if level == 1:
            print(f"\n{'='*60}")
            print(f"⚡ {title}")
            print(f"{'='*60}")
        elif level == 2:
            print(f"\n{'─'*40}")
            print(f"🔥 {title}")
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

    def measure_execution_time(self, func, *args, **kwargs):
        """Measure execution time of a function."""
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            return result, execution_time, None
        except Exception as e:
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            return None, execution_time, str(e)

    def test_settlement_calculation_performance(self):
        """Test settlement calculation performance under load."""
        self.print_header("Settlement Calculation Performance", 2)
        
        test_cases = [
            (100, 1000),    # Small load
            (1000, 1000),   # Medium load
            (10000, 1000),  # Large load
            (100000, 100),  # Stress test
        ]
        
        results = []
        
        for num_calculations, iterations in test_cases:
            self.print_info(f"Testing {num_calculations} calculations × {iterations} iterations...")
            
            execution_times = []
            
            for _ in range(iterations):
                start_time = time.perf_counter()
                
                # Perform batch calculations
                for i in range(num_calculations):
                    deviation = 100.0 + (i % 50)  # Vary deviation
                    price = 10 + (i % 5)          # Vary price
                    calculate_settlement(deviation, price)
                
                end_time = time.perf_counter()
                execution_times.append(end_time - start_time)
            
            # Calculate statistics
            avg_time = statistics.mean(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)
            std_dev = statistics.stdev(execution_times) if len(execution_times) > 1 else 0
            
            calculations_per_second = num_calculations / avg_time
            
            self.print_success(f"Completed {num_calculations} calculations:")
            self.print_info(f"  Average time: {avg_time:.4f}s")
            self.print_info(f"  Min time: {min_time:.4f}s")
            self.print_info(f"  Max time: {max_time:.4f}s")
            self.print_info(f"  Std deviation: {std_dev:.4f}s")
            self.print_info(f"  Calculations/sec: {calculations_per_second:.0f}")
            
            results.append({
                'num_calculations': num_calculations,
                'iterations': iterations,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'std_dev': std_dev,
                'calculations_per_second': calculations_per_second
            })
        
        self.test_results['settlement_performance'] = results

    def test_anomaly_detection_performance(self):
        """Test anomaly detection performance with large datasets."""
        self.print_header("Anomaly Detection Performance", 2)
        
        detector = AnomalyDetector(self.mock_session)
        
        dataset_sizes = [100, 1000, 10000, 50000]
        
        results = []
        
        for size in dataset_sizes:
            self.print_info(f"Testing anomaly detection with {size} readings...")
            
            # Generate test dataset
            readings = []
            for i in range(size):
                # Normal readings with some outliers
                if i % 1000 == 0:  # 0.1% outliers
                    value = 1000.0 + (i % 100)  # Outlier
                else:
                    value = 100.0 + (i % 20)    # Normal range
                
                readings.append({
                    'id': f'R{i:06d}',
                    'value_kwh': value,
                    'metering_point_id': f'MP{i % 100:03d}'
                })
            
            # Measure detection performance
            start_time = time.perf_counter()
            anomalies = detector.detect_anomalies(readings, threshold_multiplier=2.0)
            end_time = time.perf_counter()
            
            execution_time = end_time - start_time
            readings_per_second = size / execution_time
            
            self.print_success(f"Processed {size} readings:")
            self.print_info(f"  Execution time: {execution_time:.4f}s")
            self.print_info(f"  Readings/sec: {readings_per_second:.0f}")
            self.print_info(f"  Anomalies detected: {len(anomalies)}")
            self.print_info(f"  Anomaly rate: {len(anomalies)/size*100:.2f}%")
            
            results.append({
                'dataset_size': size,
                'execution_time': execution_time,
                'readings_per_second': readings_per_second,
                'anomalies_detected': len(anomalies),
                'anomaly_rate': len(anomalies)/size*100
            })
        
        self.test_results['anomaly_detection_performance'] = results

    def test_edi_processing_performance(self):
        """Test EDI processing performance with multiple messages."""
        self.print_header("EDI Processing Performance", 2)
        
        parser = EDIFACTParser()
        generator = APERAKGenerator(sender_id='COMAKO')
        
        # Sample EDI message template
        edi_template = """UNB+UNOC:3+SENDER123+COMAKO+250103:1200+REF{ref:03d}'
UNH+MSG{msg:03d}+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+DOC{doc:03d}+9'
DTM+137:20250103:102'
NAD+MS+COMPANY{comp:03d}+Energy Corp+Main St 123'
LOC+172+MP{mp:03d}+Metering Point {mp}'
QTY+220:{value:.1f}:KWH'
MEA+AAE:KWH:{value:.1f}:KWH'
UNT+8+MSG{msg:03d}'
UNZ+1+REF{ref:03d}'"""

        message_counts = [10, 100, 500, 1000]
        
        results = []
        
        for count in message_counts:
            self.print_info(f"Testing EDI processing with {count} messages...")
            
            # Generate test messages
            messages = []
            for i in range(count):
                message = edi_template.format(
                    ref=i+1,
                    msg=i+1,
                    doc=i+1,
                    comp=i+1,
                    mp=i+1,
                    value=1500.0 + (i % 100)
                )
                messages.append(message)
            
            # Measure parsing performance
            start_time = time.perf_counter()
            parsed_messages = []
            
            for message in messages:
                try:
                    parsed_data = parser.parse_edi_file(message)
                    parsed_messages.append(parsed_data)
                except Exception as e:
                    self.print_warning(f"Failed to parse message: {e}")
            
            parse_time = time.perf_counter() - start_time
            
            # Measure conversion performance
            start_time = time.perf_counter()
            converted_messages = []
            
            for parsed_data in parsed_messages:
                try:
                    json_result = convert_utilmd_to_json(parsed_data)
                    converted_messages.append(json_result)
                except Exception as e:
                    self.print_warning(f"Failed to convert message: {e}")
            
            convert_time = time.perf_counter() - start_time
            
            # Measure validation performance
            start_time = time.perf_counter()
            validation_results = []
            
            for parsed_data in parsed_messages:
                try:
                    result = validate_edi_message(parsed_data)
                    validation_results.append(result)
                except Exception as e:
                    self.print_warning(f"Failed to validate message: {e}")
            
            validate_time = time.perf_counter() - start_time
            
            # Measure APERAK generation performance
            start_time = time.perf_counter()
            aperak_messages = []
            
            for parsed_data in parsed_messages:
                try:
                    aperak = generator.generate_acceptance_aperak(parsed_data)
                    aperak_messages.append(aperak)
                except Exception as e:
                    self.print_warning(f"Failed to generate APERAK: {e}")
            
            aperak_time = time.perf_counter() - start_time
            
            total_time = parse_time + convert_time + validate_time + aperak_time
            messages_per_second = count / total_time
            
            self.print_success(f"Processed {count} EDI messages:")
            self.print_info(f"  Parse time: {parse_time:.4f}s")
            self.print_info(f"  Convert time: {convert_time:.4f}s")
            self.print_info(f"  Validate time: {validate_time:.4f}s")
            self.print_info(f"  APERAK time: {aperak_time:.4f}s")
            self.print_info(f"  Total time: {total_time:.4f}s")
            self.print_info(f"  Messages/sec: {messages_per_second:.1f}")
            self.print_info(f"  Success rate: {len(parsed_messages)/count*100:.1f}%")
            
            results.append({
                'message_count': count,
                'parse_time': parse_time,
                'convert_time': convert_time,
                'validate_time': validate_time,
                'aperak_time': aperak_time,
                'total_time': total_time,
                'messages_per_second': messages_per_second,
                'success_rate': len(parsed_messages)/count*100
            })
        
        self.test_results['edi_processing_performance'] = results

    def test_concurrent_processing(self):
        """Test concurrent processing capabilities."""
        self.print_header("Concurrent Processing Performance", 2)
        
        def settlement_worker(worker_id: int, num_calculations: int):
            """Worker function for concurrent settlement calculations."""
            start_time = time.perf_counter()
            
            for i in range(num_calculations):
                deviation = 100.0 + (i % 50)
                price = 10 + (i % 5)
                calculate_settlement(deviation, price)
            
            end_time = time.perf_counter()
            return {
                'worker_id': worker_id,
                'calculations': num_calculations,
                'execution_time': end_time - start_time
            }
        
        thread_counts = [1, 2, 4, 8, 16]
        calculations_per_thread = 1000
        
        results = []
        
        for num_threads in thread_counts:
            self.print_info(f"Testing with {num_threads} concurrent threads...")
            
            start_time = time.perf_counter()
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                
                for i in range(num_threads):
                    future = executor.submit(settlement_worker, i, calculations_per_thread)
                    futures.append(future)
                
                # Wait for all threads to complete
                thread_results = []
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    thread_results.append(result)
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            
            total_calculations = num_threads * calculations_per_thread
            calculations_per_second = total_calculations / total_time
            
            avg_thread_time = statistics.mean([r['execution_time'] for r in thread_results])
            
            self.print_success(f"Concurrent processing with {num_threads} threads:")
            self.print_info(f"  Total time: {total_time:.4f}s")
            self.print_info(f"  Avg thread time: {avg_thread_time:.4f}s")
            self.print_info(f"  Total calculations: {total_calculations}")
            self.print_info(f"  Calculations/sec: {calculations_per_second:.0f}")
            self.print_info(f"  Speedup factor: {calculations_per_second/(calculations_per_thread/avg_thread_time):.2f}x")
            
            results.append({
                'num_threads': num_threads,
                'total_time': total_time,
                'avg_thread_time': avg_thread_time,
                'total_calculations': total_calculations,
                'calculations_per_second': calculations_per_second,
                'speedup_factor': calculations_per_second/(calculations_per_thread/avg_thread_time)
            })
        
        self.test_results['concurrent_processing'] = results

    async def test_async_processing_performance(self):
        """Test asynchronous processing performance."""
        self.print_header("Async Processing Performance", 2)
        
        async def async_settlement_worker(worker_id: int, num_calculations: int):
            """Async worker function for settlement calculations."""
            start_time = time.perf_counter()
            
            for i in range(num_calculations):
                deviation = 100.0 + (i % 50)
                price = 10 + (i % 5)
                calculate_settlement(deviation, price)
                
                # Simulate async I/O operation
                if i % 100 == 0:
                    await asyncio.sleep(0.001)  # 1ms delay
            
            end_time = time.perf_counter()
            return {
                'worker_id': worker_id,
                'calculations': num_calculations,
                'execution_time': end_time - start_time
            }
        
        async_counts = [1, 5, 10, 20, 50]
        calculations_per_task = 500
        
        results = []
        
        for num_tasks in async_counts:
            self.print_info(f"Testing with {num_tasks} async tasks...")
            
            start_time = time.perf_counter()
            
            # Create and run async tasks
            tasks = []
            for i in range(num_tasks):
                task = async_settlement_worker(i, calculations_per_task)
                tasks.append(task)
            
            task_results = await asyncio.gather(*tasks)
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            
            total_calculations = num_tasks * calculations_per_task
            calculations_per_second = total_calculations / total_time
            
            avg_task_time = statistics.mean([r['execution_time'] for r in task_results])
            
            self.print_success(f"Async processing with {num_tasks} tasks:")
            self.print_info(f"  Total time: {total_time:.4f}s")
            self.print_info(f"  Avg task time: {avg_task_time:.4f}s")
            self.print_info(f"  Total calculations: {total_calculations}")
            self.print_info(f"  Calculations/sec: {calculations_per_second:.0f}")
            
            results.append({
                'num_tasks': num_tasks,
                'total_time': total_time,
                'avg_task_time': avg_task_time,
                'total_calculations': total_calculations,
                'calculations_per_second': calculations_per_second
            })
        
        self.test_results['async_processing'] = results

    def test_memory_usage_patterns(self):
        """Test memory usage patterns under load."""
        self.print_header("Memory Usage Patterns", 2)
        
        import psutil
        import gc
        
        process = psutil.Process()
        
        # Test 1: Settlement calculation memory usage
        self.print_info("Testing settlement calculation memory usage...")
        
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Perform many calculations
        for i in range(100000):
            deviation = 100.0 + (i % 50)
            price = 10 + (i % 5)
            result = calculate_settlement(deviation, price)
        
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Force garbage collection
        gc.collect()
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        self.print_success("Settlement calculation memory usage:")
        self.print_info(f"  Initial memory: {initial_memory:.2f} MB")
        self.print_info(f"  Peak memory: {peak_memory:.2f} MB")
        self.print_info(f"  Final memory: {final_memory:.2f} MB")
        self.print_info(f"  Memory increase: {peak_memory - initial_memory:.2f} MB")
        self.print_info(f"  Memory after GC: {final_memory - initial_memory:.2f} MB")
        
        # Test 2: EDI processing memory usage
        self.print_info("Testing EDI processing memory usage...")
        
        parser = EDIFACTParser()
        
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process many EDI messages
        edi_message = """UNB+UNOC:3+SENDER123+COMAKO+250103:1200+REF001'
UNH+MSG001+UTILMD:D:03B:UN:EEG+1.1e'
BGM+E01+DOC123+9'
DTM+137:20250103:102'
NAD+MS+COMPANY123+Energy Corp+Main St 123'
LOC+172+MP001+Metering Point 1'
QTY+220:1500.5:KWH'
MEA+AAE:KWH:1500.5:KWH'
UNT+8+MSG001'
UNZ+1+REF001'"""

        parsed_messages = []
        for i in range(1000):
            try:
                parsed_data = parser.parse_edi_file(edi_message)
                parsed_messages.append(parsed_data)
            except Exception:
                pass
        
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Clear references and force GC
        parsed_messages.clear()
        gc.collect()
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        self.print_success("EDI processing memory usage:")
        self.print_info(f"  Initial memory: {initial_memory:.2f} MB")
        self.print_info(f"  Peak memory: {peak_memory:.2f} MB")
        self.print_info(f"  Final memory: {final_memory:.2f} MB")
        self.print_info(f"  Memory increase: {peak_memory - initial_memory:.2f} MB")
        self.print_info(f"  Memory after GC: {final_memory - initial_memory:.2f} MB")
        
        self.test_results['memory_usage'] = {
            'settlement_calculation': {
                'initial_memory': initial_memory,
                'peak_memory': peak_memory,
                'final_memory': final_memory,
                'memory_increase': peak_memory - initial_memory
            }
        }

    def print_performance_summary(self):
        """Print comprehensive performance test summary."""
        self.print_header("Performance Test Summary", 1)
        
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        self.print_info(f"Performance tests completed in {duration.total_seconds():.2f} seconds")
        self.print_info(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.print_info(f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Settlement performance summary
        if 'settlement_performance' in self.test_results:
            settlement_results = self.test_results['settlement_performance']
            max_throughput = max(r['calculations_per_second'] for r in settlement_results)
            
            self.print_header("Settlement Performance Summary", 2)
            self.print_info(f"Maximum throughput: {max_throughput:.0f} calculations/sec")
            
            for result in settlement_results:
                self.print_info(f"  {result['num_calculations']} calculations: {result['calculations_per_second']:.0f}/sec")
        
        # Anomaly detection performance summary
        if 'anomaly_detection_performance' in self.test_results:
            anomaly_results = self.test_results['anomaly_detection_performance']
            max_throughput = max(r['readings_per_second'] for r in anomaly_results)
            
            self.print_header("Anomaly Detection Performance Summary", 2)
            self.print_info(f"Maximum throughput: {max_throughput:.0f} readings/sec")
            
            for result in anomaly_results:
                self.print_info(f"  {result['dataset_size']} readings: {result['readings_per_second']:.0f}/sec")
        
        # EDI processing performance summary
        if 'edi_processing_performance' in self.test_results:
            edi_results = self.test_results['edi_processing_performance']
            max_throughput = max(r['messages_per_second'] for r in edi_results)
            
            self.print_header("EDI Processing Performance Summary", 2)
            self.print_info(f"Maximum throughput: {max_throughput:.1f} messages/sec")
            
            for result in edi_results:
                self.print_info(f"  {result['message_count']} messages: {result['messages_per_second']:.1f}/sec")
        
        # Concurrent processing summary
        if 'concurrent_processing' in self.test_results:
            concurrent_results = self.test_results['concurrent_processing']
            max_speedup = max(r['speedup_factor'] for r in concurrent_results)
            
            self.print_header("Concurrent Processing Summary", 2)
            self.print_info(f"Maximum speedup: {max_speedup:.2f}x")
            
            for result in concurrent_results:
                self.print_info(f"  {result['num_threads']} threads: {result['speedup_factor']:.2f}x speedup")
        
        # Overall performance rating
        self.print_header("Overall Performance Rating", 2)
        
        # Calculate performance score based on throughput
        score = 0
        if 'settlement_performance' in self.test_results:
            max_settlement = max(r['calculations_per_second'] for r in self.test_results['settlement_performance'])
            score += min(max_settlement / 10000, 1.0) * 25  # Max 25 points
        
        if 'anomaly_detection_performance' in self.test_results:
            max_anomaly = max(r['readings_per_second'] for r in self.test_results['anomaly_detection_performance'])
            score += min(max_anomaly / 1000, 1.0) * 25  # Max 25 points
        
        if 'edi_processing_performance' in self.test_results:
            max_edi = max(r['messages_per_second'] for r in self.test_results['edi_processing_performance'])
            score += min(max_edi / 100, 1.0) * 25  # Max 25 points
        
        if 'concurrent_processing' in self.test_results:
            max_concurrent = max(r['speedup_factor'] for r in self.test_results['concurrent_processing'])
            score += min(max_concurrent / 4, 1.0) * 25  # Max 25 points
        
        if score >= 80:
            self.print_success(f"🎉 Excellent Performance: {score:.1f}/100")
        elif score >= 60:
            self.print_success(f"✅ Good Performance: {score:.1f}/100")
        elif score >= 40:
            self.print_warning(f"⚠️ Acceptable Performance: {score:.1f}/100")
        else:
            self.print_warning(f"⚠️ Performance Needs Improvement: {score:.1f}/100")

    async def run_performance_tests(self):
        """Run all performance tests."""
        self.print_header("CoMaKo Performance & Stress Test Suite", 1)
        
        self.print_info("Running comprehensive performance tests...")
        self.print_info("• Settlement Calculation Performance")
        self.print_info("• Anomaly Detection Performance")
        self.print_info("• EDI Processing Performance")
        self.print_info("• Concurrent Processing Performance")
        self.print_info("• Async Processing Performance")
        self.print_info("• Memory Usage Patterns")
        
        # Run all performance tests
        self.test_settlement_calculation_performance()
        self.test_anomaly_detection_performance()
        self.test_edi_processing_performance()
        self.test_concurrent_processing()
        await self.test_async_processing_performance()
        self.test_memory_usage_patterns()
        
        # Print summary
        self.print_performance_summary()


async def main():
    """Main performance test execution function."""
    performance_test = CoMaKoPerformanceTest()
    await performance_test.run_performance_tests()


if __name__ == "__main__":
    # Run the performance tests
    asyncio.run(main())
