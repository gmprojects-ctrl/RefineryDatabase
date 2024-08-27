# RefineryDatabase
Simple Refinery Database extracted out of Wikipedia list of Refineries https://en.wikipedia.org/wiki/List_of_oil_refineries

# Methods
The GET method has filter that can filter using argument i.e GET route/filter?region=Europe
The POST method has addrefinery i.e route/addrefinery +BODY
The DELETE method has deleterefinery i.e route/deleterefinery/id
The PATCH method has updaterefinery i.e rout/updaterefinery/id and body to change

# Example refineries to add
```json
{
    "region" : "Pangea",
    "country": "Osea",
    "refinery" : "foo refinery",
    "capacity" : 400,
    "status" : "active"
}
```
# Todo
- [X] Implement SQL Alchemy ORM
- [X] Rename Regions into Queriable Regions (i.e not North & Central America -> North America)
- [] Refactor code to implement proper testing.
- [X] Implement basic GET method based on continent and country
- [] Implement a POST method
- [] Implement a delete method
