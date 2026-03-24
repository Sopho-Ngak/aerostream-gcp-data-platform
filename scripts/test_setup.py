#!/usr/bin/env python3
"""
Test script to verify all components are working
"""

import socket
import requests
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICES = {
    "zookeeper": (2181, "tcp"),
    "kafka": (9092, "tcp"),
    "namenode": (9870, "http"),
    "datanode": (9864, "http"),
    "spark-master": (8080, "http"),
    "airflow-webserver": (8082, "http"),
    "superset": (8088, "http")
}

def check_tcp_connection(host, port):
    """Check if TCP port is open"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.error(f"Error checking {host}:{port} - {e}")
        return False

def check_http_endpoint(host, port, path="/"):
    """Check if HTTP endpoint returns 200"""
    try:
        response = requests.get(f"http://{host}:{port}{path}", timeout=5)
        return response.status_code == 200
    except:
        return False

def test_service(service_name, port, check_type):
    """Test individual service"""
    logger.info(f"Testing {service_name}...")
    
    if check_type == "tcp":
        success = check_tcp_connection(service_name, port)
    else:
        success = check_http_endpoint(service_name, port)
    
    if success:
        logger.info(f"✅ {service_name} is healthy")
    else:
        logger.error(f"❌ {service_name} is not responding")
    
    return success

def main():
    logger.info("🚀 Starting system health check...")
    
    # Give services time to fully start
    time.sleep(10)
    
    results = []
    for service, (port, check_type) in SERVICES.items():
        success = test_service(service, port, check_type)
        results.append((service, success))
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("HEALTH CHECK SUMMARY")
    logger.info("="*50)
    
    all_success = True
    for service, success in results:
        status = "✅" if success else "❌"
        logger.info(f"{status} {service}")
        if not success:
            all_success = False
    
    if all_success:
        logger.info("\n✅ All systems are go! Ready to start the pipeline.")
    else:
        logger.error("\n❌ Some services are not healthy. Check logs above.")

if __name__ == "__main__":
    main()