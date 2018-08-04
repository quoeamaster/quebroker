Feature:
    To mimic the sync of cluster status between broker(s). In this test-suite;
    will test the "cluster status" sync based on a mocking broker. In order to
    test the real interaction between 2 or more broker(s); would need to do an
    integration test instead... (pending)

    Assumptions for the feature test:
    - a mocking broker is available to generate needs to sync cluster status
    - the sync operation is controlled by the master broker; hence
        acknowledgments from other broker(s) should be sent to it

    Major use cases (Scenario 1):
    - create a mocking broker and send out a cluster status sync request to
        the rest of broker(s) within the cluster (in this case, our
        testing broker would be tested on whether the sync operation works or not)
    - In general the rule is all cluster status update(s) must be done by the
        MASTER broker; hence the sync should be in only 1 direction =>
        from Master broker to other broker(s)

    Scenario: 1) sync between Master
        Given a webservice url "https://glosbe.com/gapi/tm"
        When parameters provided as "[from:eng,dest:eng,format:json,phrase:soccer,page:1,pretty:true]"
        Then calling the api would resulted a Json response
        And "football" is found within the response

