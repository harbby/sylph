package ideal.sylph.runner.flink.actuator;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;

public class JobParameter
{
    @JsonProperty("queue")
    private String queue = "default";

    private int taskManagerMemoryMb = 1024;
    private int taskManagerCount = 2;
    private int taskManagerSlots = 2;
    private int jobManagerMemoryMb = 1024;
    private Set<String> appTags;
    private String yarnJobName;

    public JobParameter()
    {
    }

    public JobParameter setYarnJobName(String yarnJobName)
    {
        this.yarnJobName = yarnJobName;
        return this;
    }

    public String getYarnJobName()
    {
        return yarnJobName;
    }

    public JobParameter taskManagerCount(int taskManagerCount)
    {
        this.taskManagerCount = taskManagerCount;
        return this;
    }

    public JobParameter taskManagerMemoryMb(int taskManagerMemoryMb)
    {
        this.taskManagerMemoryMb = taskManagerMemoryMb;
        return this;
    }

    public JobParameter taskManagerSlots(int taskManagerSlots)
    {
        this.taskManagerSlots = taskManagerSlots;
        return this;
    }

    public JobParameter jobManagerMemoryMb(int jobManagerMemoryMb)
    {
        this.jobManagerMemoryMb = jobManagerMemoryMb;
        return this;
    }

    public JobParameter appTags(Set<String> appTags)
    {
        this.appTags = appTags;
        return this;
    }

    public Set<String> getAppTags()
    {
        return appTags;
    }

    public int getJobManagerMemoryMb()
    {
        return jobManagerMemoryMb;
    }

    public int getTaskManagerSlots()
    {
        return taskManagerSlots;
    }

    public int getTaskManagerCount()
    {
        return taskManagerCount;
    }

    public int getTaskManagerMemoryMb()
    {
        return taskManagerMemoryMb;
    }

    public JobParameter queue(String queue)
    {
        this.queue = queue;
        return this;
    }

    /**
     * The name of the queue to which the application should be submitted
     *
     * @return queue
     **/
    @JsonProperty("queue")
    public String getQueue()
    {
        return queue;
    }

    public void setQueue(String queue)
    {
        this.queue = queue;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobParameter jobParameter = (JobParameter) o;
        return Objects.equals(this.queue, jobParameter.queue) &&
                Objects.equals(this.taskManagerCount, jobParameter.taskManagerCount) &&
                Objects.equals(this.taskManagerMemoryMb, jobParameter.taskManagerMemoryMb);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queue, taskManagerMemoryMb, taskManagerCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queue", queue)
                .add("memory", taskManagerMemoryMb)
                .add("vCores", taskManagerCount)
                .toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(Object o)
    {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}
