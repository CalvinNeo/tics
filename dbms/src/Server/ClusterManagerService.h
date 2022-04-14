#pragma once

#include <Common/Timer.h>
#include <common/logger_useful.h>

#include <boost/noncopyable.hpp>

namespace DB
{
class Context;
class BackgroundProcessingPool;

class ClusterManagerService : private boost::noncopyable
{
public:
    ClusterManagerService(Context & context_, const std::string & config_path);
    ~ClusterManagerService();

private:
    Context & context;
    Timer timer;
    Poco::Logger * log;
};


} // namespace DB