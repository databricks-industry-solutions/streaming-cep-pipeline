import axios from 'axios';

const API_BASE = '/api';

export const api = {
  listRules: async (): Promise<string[]> => {
    const response = await axios.get(`${API_BASE}/rules`);
    return response.data;
  },

  getRule: async (filename: string): Promise<any> => {
    const response = await axios.get(`${API_BASE}/rules/${filename}`);
    return response.data;
  },

  saveRule: async (filename: string, content: any): Promise<any> => {
    const response = await axios.post(`${API_BASE}/rules/${filename}`, { content });
    return response.data;
  },
};
