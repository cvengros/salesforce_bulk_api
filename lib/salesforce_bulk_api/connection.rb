module SalesforceBulkApi
require 'timeout'

  class Connection
    include Concerns::Throttling

    @@XML_HEADER = '<?xml version="1.0" encoding="utf-8" ?>'
    @@XML_REQUEST_HEADER = {'Content-Type' => 'application/xml; charset=utf-8'}

    def initialize(client)
      @client=client
      @@PATH_PREFIX = "/services/async/#{@client.options[:api_version]}/"
    end

    def session_header
      {'X-SFDC-Session' => @client.options[:oauth_token]}
    end

    def post_xml(path, xml, options={})
      path = "#{@@PATH_PREFIX}#{path}"
      headers = @@XML_REQUEST_HEADER

      response = nil
      # do the request
      with_retries do
        begin
          response = @client.post(path, xml, headers.merge(session_header))
        rescue JSON::ParserError => e
          if e.message.index('ExceededQuota')
            raise "You've run out of sfdc batch api quota. Original error: #{e}\n #{e.backtrace}"
          end
          raise e
        end
      end
      response.body
    end

    def get_request(path, headers)
      path = "#{@@PATH_PREFIX}#{path}"

      response = nil
      with_retries do
        response = @client.get(path, {}, headers.merge(session_header))
      end
      response.body
    end

    def https(host)
      req = Net::HTTP.new(host, 443) # 6109
      req.use_ssl = true # false
      req.verify_mode = OpenSSL::SSL::VERIFY_NONE
      req
    end

    def parse_instance()
      @instance = @server_url.match(/https:\/\/[a-z]{2}[0-9]{1,2}/).to_s.gsub("https://","")
      @instance = @server_url.split(".salesforce.com")[0].split("://")[1] if @instance.nil? || @instance.empty?
      return @instance
    end

    def counters
      {
        get: get_counters[:get],
        post: get_counters[:post]
      }
    end

    private

    def parse_xml(xml)
      parsed = nil
      begin
        parsed = XmlSimple.xml_in(xml)
      rescue => e
        @logger.error "Error parsing xml: #{xml}\n#{e}\n#{e.backtrace}"
        raise
      end

      return parsed
    end

    def with_retries
      i = 0
      begin
        yield
      rescue => e
        i += 1
        if i < 3
          @logger.warn "Retrying, got error: #{e}, #{e.backtrace}" if @logger
          retry
        else
          @logger.error "Failed 3 times, last error: #{e}, #{e.backtrace}" if @logger
          raise
        end
      end
    end

    def get_counters
      @counters ||= Hash.new(0)
    end

    def count(http_method)
      get_counters[http_method] += 1
    end

  end

end
