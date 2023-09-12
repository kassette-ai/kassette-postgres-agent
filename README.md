### kassette-postgres-agent 
kassette-postgres-agent intended to run on customer environment and collect neccesary Data and push it back to kassette-server
Camunda is the first integration supported by the agent. 
Agent accesses camunda database directly and polls history tables. 
Data sent to the kassette-server via REST API 

### run kassette agent locally in docker
## Prerequisites
1. checkout and run camunda-diy project. https://github.com/metaops-solutions/camunda-diy
2. checkout and run kassette-server project. https://github.com/kassette-ai/kassette-server
3. run docker-compose up

### Releasing

We will use git tags to track the versions 
```
git tag -a v1.0.0 -m "First tagged release of the kassette-postgres-agent"
git push --tags
```


### Usage
