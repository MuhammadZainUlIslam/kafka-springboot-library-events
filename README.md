📘 Kafka Spring Boot Library Events Producer
📌 Project Overview
This project is a Spring Boot-based Kafka Producer application designed for a Library Management System. It demonstrates how real-world event-driven architecture works using Apache Kafka to publish library events asynchronously to a Kafka topic.

The system is built with a focus on clean architecture, scalability, and production-ready design principles, and serves as the foundation for a full distributed microservices-based library system.

✅ What Has Been Implemented (Current Progress)
🚀 1. Spring Boot Kafka Producer Setup
Configured Spring Boot application with Java 21

Integrated Apache Kafka producer dependencies

Configured Kafka bootstrap servers for different environments (local, dev, prod)

Implemented producer configuration using:

IntegerSerializer (key)

StringSerializer (value)

📡 2. REST API for Event Publishing
Developed REST controller:

POST /v1/libraryevent
Accepts LibraryEvent JSON payload

Publishes events to Kafka topic: library-events

Supports logging for request tracking and debugging

📦 3. Domain Model Design
Implemented clean domain structure using Java Records:

LibraryEvent

libraryEventId

libraryEventType (NEW / UPDATE)

Book

Book

bookId

bookName

bookAuthor

LibraryEventType enum:

NEW

UPDATE

This ensures a lightweight, immutable, and clean data model design.

⚙️ 4. Kafka Topic Auto Creation
Configured automatic topic creation using Spring Kafka Admin

Topic properties are externalized:

Topic name: library-events

Partitions: 3

Replication factor: 1

🔄 5. Producer Implementation (Multiple Approaches)
The project includes multiple Kafka producer strategies:

Asynchronous producer (currently active)

Synchronous producer (implemented but commented)

Advanced producer approach (prepared for future enhancement)

This demonstrates flexibility in handling event publishing strategies.

🧪 6. Integration Testing (Initial Implementation)
Implemented TestRestTemplate based integration testing

Verified REST endpoint functionality:

Request serialization

HTTP status validation (201 CREATED)

Basic end-to-end validation of controller layer

🧰 7. Utility & Test Data Management
Created TestUtil class for reusable test objects

Centralized test data creation:

valid LibraryEvent

update events

invalid book scenarios (for future validation testing)

🧭 Architecture Highlights
Layered architecture (Controller → Producer → Kafka)

Event-driven design using Kafka

Clean separation of domain and service layers

Externalized configuration using YAML profiles (local/dev/prod)

📌 Future Enhancements (Planned Work)
🧪 1. Complete Integration Testing
Extend integration tests to cover:

All producer methods (sync, async, advanced)

Edge cases (invalid payloads, null IDs, update scenarios)

Kafka message verification using test containers

🧬 2. Unit Testing
After completing integration tests:

Service layer unit tests will be added

Mocking Kafka producer using Mockito

Validation logic testing

📥 3. Kafka Consumer Service
A separate Consumer microservice will be created

Responsibilities:

Consume library events from Kafka

Process and store events

Handle NEW and UPDATE event types separately

🗄️ 4. Database Integration
Integration of persistent storage (likely MySQL/PostgreSQL)

Store:

Library events

Book data

Enable event sourcing style tracking

🛡️ 5. Advanced Validations
Strong request validation using:

Bean Validation (Jakarta Validation)

Custom validation rules for:

Book ID constraints

Event type rules (NEW vs UPDATE consistency)

Input sanitization and error handling improvements

🔐 6. Security Enhancements (SSL & Secure Communication)
Enable SSL/TLS for Kafka communication

Secure REST APIs using HTTPS

Future integration of:

Spring Security

Role-based access control (RBAC)

Secure producer-consumer communication in distributed environment

🎯 Project Goal
The ultimate goal of this system is to evolve into a full-scale event-driven microservices architecture, simulating real-world enterprise systems where:

Services communicate via Kafka

Data is processed asynchronously

System is scalable, secure, and resilient

🚀 Summary
This project currently demonstrates:

Kafka producer setup in Spring Boot

REST API event publishing

Domain modeling using modern Java features

Basic integration testing

Multi-environment configuration support

And is actively evolving toward:

Full test coverage (integration + unit testing)

Kafka consumer microservices

Database persistence layer

Enterprise-grade security (SSL + validation)


## 👨‍💻 Author

**Muhammad Zain Ul Islam**  
Software Engineer | Spring Boot Developer
- GitHub: https://github.com/MuhammadZainUlIslam  