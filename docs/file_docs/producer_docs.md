# Own docs for producer.py script

- Producer.py have THREE responsibilitys, they are:
    - Poll github events API 
    - Filter away events I dont care about(see config.py) 
        - SE: Filtrera bort events jag ej bryr mig om
    - Send relevant events to Kafka 
        - SE: Skicka relevanta events till Kafka

*Nothing more and nothing less*