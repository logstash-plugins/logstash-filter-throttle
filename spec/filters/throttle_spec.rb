require "logstash/devutils/rspec/spec_helper"
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
  
  describe "max_counter exceeded" do
    config <<-CONFIG
      filter {
        throttle {
          period => 60
          after_count => 1
          max_counters => 2
          key => "%{message}"
          add_tag => [ "throttled" ]
        }
      }
    CONFIG

    events = [{
      "message" => "foo"
    }, {
      "message" => "bar"
    }, {
      "message" => "poo"
    }, {
      "message" => "foo"
    }]

    sample events do
      insist { subject[3].get("tags") } == nil
    end
  end

end # LogStash::Filters::Throttle

