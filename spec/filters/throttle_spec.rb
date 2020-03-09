require "logstash/devutils/rspec/spec_helper"
require "insist"
require "logstash/filters/throttle"

describe LogStash::Filters::Throttle do

  describe "no before_count" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 2
          key => "%{host}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    event = {
      "host" => "server1"
    }

    sample event do
      insist { subject.get("tags") } == nil
    end
  end
  
  describe "before_count throttled" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          before_count => 2
          after_count => 3
          key => "%{host}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    event = {
      "host" => "server1"
    }

    sample event do
      insist { subject.get("tags") } == [ "throttled" ]
    end
  end
  
  describe "before_count exceeded" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          before_count => 2
          after_count => 3
          key => "%{host}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "host" => "server1"
    }, {
      "host" => "server1"
    }]

    sample events do
      insist { subject[0].get("tags") } == [ "throttled" ]
      insist { subject[1].get("tags") } == nil
    end
  end
  
  describe "after_count exceeded" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          before_count => 2
          after_count => 3
          key => "%{host}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "host" => "server1"
    }, {
      "host" => "server1"
    }, {
      "host" => "server1"
    }, {
      "host" => "server1"
    }]

    sample events do
      insist { subject[0].get("tags") } == [ "throttled" ]
      insist { subject[1].get("tags") } == nil
      insist { subject[2].get("tags") } == nil
      insist { subject[3].get("tags") } == [ "throttled" ]
    end
  end
  
  describe "different keys" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 2
          key => "%{host}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "host" => "server1"
    }, {
      "host" => "server2"
    }, {
      "host" => "server3"
    }, {
      "host" => "server4"
    }]

    sample events do
      subject.each { | s |
        insist { s.get("tags") } == nil
      }
    end
  end
  
  describe "composite key" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 1
          key => "%{host}%{message}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "host" => "server1",
      "message" => "foo"
    }, {
      "host" => "server1",
      "message" => "bar"
    }, {
      "host" => "server2",
      "message" => "foo"
    }, {
      "host" => "server2",
      "message" => "bar"
    }]

    sample events do
      subject.each { | s |
        insist { s.get("tags") } == nil
      }
    end
  end

  describe "correct timeslot assigned/calculated, after_count exceeded" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 1
          key => "%{message}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "@timestamp" => "2016-07-09T00:05:00.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:05:59.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:10:33.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:10:34.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:00:00.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:00:45.000Z",
      "message"    => "server1"
    }]

    sample events do
      insist { subject[0].get("tags") } == nil
      insist { subject[1].get("tags") } == [ "throttled" ]
      insist { subject[2].get("tags") } == nil
      insist { subject[3].get("tags") } == [ "throttled" ]
      insist { subject[4].get("tags") } == nil
      insist { subject[5].get("tags") } == [ "throttled" ]
    end
  end

  describe "asynchronous input, after_count exceeded" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 1
          key => "%{message}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "@timestamp" => "2016-07-09T00:01:00.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:00:30.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:01:59.000Z",
      "message"    => "server1"
    }, {
      "@timestamp" => "2016-07-09T00:00:59.000Z",
      "message"    => "server1"
    }]

    sample events do
      insist { subject[0].get("tags") } == nil
      insist { subject[1].get("tags") } == nil
      insist { subject[2].get("tags") } == [ "throttled" ]
      insist { subject[3].get("tags") } == [ "throttled" ]
    end
  end

end # LogStash::Filters::Throttle
