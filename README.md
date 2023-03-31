# DataStore API

This API provides a simple key-value data store with support for basic operations like SET, GET, QPUSH, and QPOP.

## Language and Frameworks
- Python - 3.10
- Flask - 2.2

## Getting started

1. Clone this repository to your local machine.
2. Install the required dependencies using pip install -r requirements.txt.
3. Run the application using python app.py.
4. The API will now be running on http://localhost:5000.

## Usage

All methods are routed to the same endpoint `/`, i.e. there is no need to add any endpoint after the URL. 
All methods can be accessed by `POST` request only.

### SET
Writes the value to the datastore using the key and according to the specified parameters.

**Request body:**

```
{
    "command": "SET key1 109 EX 10 NX"
}
```

**Response body:**

```
{
    "success": "value set successfully"
}
```

### GET

Returns the value stored using the specified key.

**Request body:**

```
{
    "command": "GET key1"
}
```

**Response body:**

```
{
    "value": 109
}
```

### QPUSH

Creates a queue if not already created and appends values to it.

**Request body:**

```
{
    "command": "QPUSH list_a 10 19 21"
}
```

**Response body:**

```
{
    "success": "values added to queue"
}
```

### QPOP

Returns the last inserted value from the queue.

**Request body:**

```
{
    "command": "QPOP list_a"
}
```

**Response body:**

```
{
    "value": 21
}
```

### BQPOP

Returns the last inserted value from the queue with a blocking timeout in case of an empty queue

**Request body:**

```
{
    "command": "BQPOP list_a 10"
}
```

**Response body:**

```
{
    "value": 21
}
```

## Assumptions

1. The key-value pairs correspond to the str-int data types, i.e. keys are of type string while values are always integer.
2. The key in a `GET` command the queue_key in the `QPOP` command are different, i.e. a key 'k' with value 12 and a key 'k' with values [12, 1, 43] are different.