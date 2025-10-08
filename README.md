# qParser
A simple parser based on Apache Calcite for ParSEval. 

## Dependency
- Java Development Kit(JDK) (version 17)
- Apache Maven 3.8.9

Follow the installation instructions for your OS to install JDK and Maven.

## Installation

1. Clone the Project

```bash
git clone git@github.com:sfu-db/qParser.git
cd qParser
```

2. Build the Project

Use Maven to compile and package the project. This will also download and manage all dependencies defined in the pom.xml file.

```bash
mvn compile
mvn package
```

## Example Input
```sql
{
  "ddl": ["CREATE TABLE ...", "CREATE TABLE ..."],
  "queries": ["SELECT T2.School, STRFTIME(T2.CDSCode, '%y-%d-%m') FROM schools AS T2 WHERE T2.CDSCode = 'cds001'"],
  "functions": [
    {
      "type": "SCALAR",
      "identifier": "STRFTIME",
      "parameters": ["STRING", "STRING"],
      "return_type": "STRING"
    },
    {
      "type": "SCALAR",
      "identifier": "DATE",
      "parameters": ["STRING", "STRING"],
      "return_type": "STRING"
    }
  ]
}
```
## Example Output
```sql
[
  {
    "state": "true/false",
    "plan": "",
    "help": "",
    "error": ""
  }
]
```
