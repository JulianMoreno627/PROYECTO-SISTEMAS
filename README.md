# Plataforma de Votación de Alta Disponibilidad

Este proyecto es un sistema de votación distribuido y de alta disponibilidad que integra **Apache Kafka** y **RabbitMQ** en una arquitectura híbrida orquestada con Docker Compose.

## Arquitectura
- **Kafka**: Fuente de verdad (System of Record) para votantes elegibles y votos únicos (Compactación de topics).
- **RabbitMQ**: Sistema de acciones para validación RPC síncrona y distribución de resultados en tiempo real (Fanout/Topic).
- **Microservicios**: Implementados en Node.js.

## Cómo ejecutar
1. Iniciar infraestructura: `docker-compose up --build`
2. Simular votos: `node simulate_votes.js`
3. Ver dashboards: 
   - Global: http://localhost:4000
   - Regional: http://localhost:4001
