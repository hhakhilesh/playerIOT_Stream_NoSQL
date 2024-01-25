from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def run(topic,bootstrap_servers):

    admin_config = {
        'bootstrap.servers': bootstrap_servers
    }

    admin_client = AdminClient(admin_config)

    exsiting_topics = admin_client.list_topics().topics

    print(exsiting_topics)
    
    if any(item in topic for item in exsiting_topics):
        print("topic already exists")
        return

    new_topics = [NewTopic(topic=t,num_partitions=2,replication_factor=1) for t in topic]

    admin_client.create_topics(new_topics)
    print("line22")
    exsiting_topic = admin_client.list_topics().topics
    print(exsiting_topic)

if __name__ == "__main__":
    ## Create topic based on the league/team.
    topic=["leagueA","leagueB"]

    bootstrap_servers= '192.168.178.78:9092'
    run(topic =topic,bootstrap_servers=bootstrap_servers)

